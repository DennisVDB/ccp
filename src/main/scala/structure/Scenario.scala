package structure

import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import structure.Scenario._
import structure.Scheduler.{Run, TriggerMarginCalls, scheduledMessage}
import structure.Timed.Time
import util.DataUtil.writeToCsv

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{FiniteDuration, _}
import scalaz.Scalaz._
import scalaz._

/**
  * Creates a scenario in which clearinghouses and their members can run until the timeHorizon.
  * @param ccps the simulated CCPs.
  * @param timeHorizon the last moment in time a scheduled margin call can be made.
  * @param scheduler the scheduler for sending the messages in order.
  */
case class Scenario(ccps: Set[ActorRef],
                    members: Set[ActorRef],
                    timeHorizon: Time,
                    callEvery: Time,
                    runs: Int,
                    scheduler: ActorRef)
    extends Actor {
  var movements = Map.empty[String, Map[Int, BigDecimal]]

  var runsLeft = runs
  var waitingFor = ccps.size + members.size

  val logger = Logger("Scenario")

  implicit val timeout = Timeout(60 seconds)

  override def receive: Receive = LoggingReceive {
    case Scenario.Run =>
      ccps.foreach(scheduleMarginCalls(_)(callEvery)(callEvery))

      val all = ccps ++ members
      val t = timeHorizon + FiniteDuration(2, TimeUnit.HOURS)
      all.foreach(scheduleMessage(t, _, SendData))

      scheduler ! Run

    case Movements(name, m) =>
      logger.debug(s"RECEIVED $m")

      val data = movements.getOrElse(name, Map.empty[Int, BigDecimal])
      val newData = data + (runsLeft -> m.values.sum)
      movements += name -> newData

      waitingFor -= 1

      if (waitingFor == 0) {
        runsLeft -= 1

        if (runsLeft == 0) {
          self ! WriteData
        } else {
          logger.debug(s"NEXT RUN")
          waitingFor = (ccps ++ members).size

          val all = (ccps ++ members) + scheduler
          val wait = all.toList.map(x => (x ? Reset).mapTo[Done]).sequence

          for {
            _ <- wait
          } yield self ! Scenario.Run
        }
      }

    case WriteData => writeToCsv("data")(movements)
  }

  /**
    * Sends the message to the scheduler.
    * @param time time to deliver the message.
    * @param to recipient of the message.
    * @param message the message.
    */
  private def scheduleMessage(time: Time, to: ActorRef, message: Any) =
    scheduler ! scheduledMessage(time, to, message)

  /**
    * Recursively schedules margin calls from time t up to the time horizon.
    *
    * @param t start scheduling at from this time.
    */
  @tailrec
  private def scheduleMarginCalls(receiver: ActorRef)(callEvery: Time)(
      callAt: Time): Unit = {
    if (callAt <= timeHorizon) {
      scheduleMessage(callAt, receiver, TriggerMarginCalls)

      // Schedule next
      scheduleMarginCalls(receiver)(callEvery)(callAt + callEvery)
    }
  }
}

object Scenario {
  def props[A](ccps: Set[ActorRef],
               members: Set[ActorRef],
               timeHorizon: Time,
               callEvery: Time,
               runs: Int,
               scheduler: ActorRef): Props = {
    Props(Scenario(ccps, members, timeHorizon, callEvery, runs, scheduler))
  }

  case object Run
  case class Setup(timeHorizon: Time)
  case class Ready(s: String)
  case object Reset
  case class Movements(name: String, movements: Map[Time, BigDecimal])
  case object SendData
  case object WriteData
}
