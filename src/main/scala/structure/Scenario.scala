package structure

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import market.Portfolio
import structure.Scenario._
import structure.Scheduler.{ScheduledMessage, scheduledMessage}
import structure.Timed.Time
import structure.ccp.Ccp
import structure.ccp.Ccp.TriggerMarginCalls
import util.DataUtil.{Portfolios, writeToCsv}

import scala.annotation.tailrec
import scala.collection.immutable.List
import scala.concurrent.duration.{FiniteDuration, _}

/**
  * Creates a scenario in which clearinghouses and their members can run until the timeHorizon.
  * @param ccps the simulated CCPs.
  * @param timeHorizon the last moment in time a scheduled margin call can be made.
  * @param scheduler the scheduler for sending the messages in order.
  */
case class Scenario(ccps: Set[ActorRef],
                    members: Set[ActorRef],
                    ccpMembers: Map[ActorRef, Set[ActorRef]],
                    ccpLinks: Map[ActorRef, Set[ActorRef]],
                    timeHorizon: Time,
                    callEvery: Time,
                    runs: Int,
                    genPortfolios: () => Portfolios,
                    scheduler: ActorRef)
    extends Actor {
  var movements = Map.empty[String, Map[Int, BigDecimal]]

  var runsLeft = runs
  var waitingFor = ccps.size + members.size
  var waitForSetup = members.size + ccps.size + 1

  val logger = Logger("Scenario")

  implicit val timeout = Timeout(60 seconds)

  override def receive: Receive = LoggingReceive {
    case Run =>
      val marginCalls =
        ccps.toList.flatMap(scheduledMarginCalls(_)(callEvery)(callEvery))

      val all = (ccps ++ members).toList
      val t = timeHorizon + FiniteDuration(2, TimeUnit.HOURS)
      val sendData = all.map(scheduledMessage(t, _, SendData))

      val portfolios = genPortfolios()

      val memberPortfolios = memberPortfoliosFrom(portfolios.members) _
      val ccpPortfolios = ccpPortfoliosFrom(portfolios.ccps) _

//      logger.debug(s"OK ${portfolios.members.mapValues(_.positions)}")

      members.foreach(_ ! Member.Setup)

      ccps.foreach { ccp =>
        ccp ! Ccp.Setup(memberPortfolios(ccp), ccpPortfolios(ccp))
      }

      scheduler ! Scheduler.Setup(self, marginCalls ::: sendData)

      waitForSetup = members.size + ccps.size + 1

    case Done =>
      waitForSetup -= 1
      if (waitForSetup == 0) scheduler ! Scheduler.Run

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

          self ! Scenario.Run
        }
      }

    case WriteData => writeToCsv("results_9_80_100_bis")(movements)
  }

  private def scheduledMarginCalls(receiver: ActorRef)(callEvery: Time)(
      startAt: Time): List[ScheduledMessage] = {

    @tailrec
    def go(callAt: Time, acc: List[ScheduledMessage]): List[ScheduledMessage] = {
      if (callAt > timeHorizon) acc
      else
        go(callAt + callEvery,
           scheduledMessage(callAt, receiver, TriggerMarginCalls) :: acc)
    }

    go(startAt, List.empty)
  }

  private def memberPortfoliosFrom(
      generatedPortfolios: Map[ActorRef, Portfolio])(
      ccp: ActorRef): Map[ActorRef, Portfolio] = {
    val members = ccpMembers.getOrElse(ccp, throw new IllegalStateException())
    (for {
      m <- members
      portfolio = generatedPortfolios.getOrElse(
        m,
        throw new IllegalStateException)
    } yield m -> portfolio).toMap
  }

  private def ccpPortfoliosFrom(
      generatedPortfolios: Map[ActorRef, Map[ActorRef, Portfolio]])(
      ccp: ActorRef): Map[ActorRef, Portfolio] = {
    generatedPortfolios.getOrElse(ccp, throw new IllegalStateException())
  }
}

object Scenario {
  def props[A](ccps: Set[ActorRef],
               members: Set[ActorRef],
               ccpMembers: Map[ActorRef, Set[ActorRef]],
               ccpLinks: Map[ActorRef, Set[ActorRef]],
               timeHorizon: Time,
               callEvery: Time,
               runs: Int,
               genPortfolios: () => Portfolios,
               scheduler: ActorRef): Props = {
    Props(
      Scenario(ccps,
               members,
               ccpMembers,
               ccpLinks,
               timeHorizon,
               callEvery,
               runs,
               genPortfolios,
               scheduler))
  }

  case object Run
  case object Done
  case class Ready(s: String)
  case class Movements(name: String, movements: Map[Time, BigDecimal])
  case object SendData
  case object WriteData
}
