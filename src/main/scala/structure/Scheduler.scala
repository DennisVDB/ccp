package structure

import akka.actor.{Actor, ActorRef, Props}
import structure.Scheduler.{Release, Run, ScheduledMessage}
import structure.Timed.Time

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by dennis on 8/12/16.
  */
case class Scheduler(stepTime: FiniteDuration) extends Actor {
  private val messages =
    new mutable.HashMap[Time, mutable.Set[ScheduledMessage]]
    with mutable.MultiMap[Time, ScheduledMessage]

  private var time: Time = 0
  private var lastMarginCall: Time = 0

  def receive: Receive = {
    case Run =>
      context.system.scheduler.scheduleOnce(stepTime)(self ! Release)

    case Release =>
      release()
      context.system.scheduler.scheduleOnce(stepTime)(self ! Release)

    case s @ ScheduledMessage(_, TimedMessage(t, _)) =>
      messages.addBinding(t, s)
  }

  /**
    * Sends the messages due, in order of the arrival time.
    */
  def release(): Unit = {
    val toSend =
      messages
        .filterKeys(_ <= time)
        .toSeq
        .sortBy(_._1) // Order by time
        .map(_._2) // Select messages to send

    toSend.foreach(_.foreach(m => m.receiver ! m.message)) // Send messages

    (0 to time).foreach(t => messages -= t) // Remove sent messages

    time += 1
  }
}

object Scheduler {
  case object Run
  case object TriggerMarginCalls
  case object Release
  case class ScheduledMessage(receiver: ActorRef, message: TimedMessage)

  def scheduleMessageWith(
      s: ActorRef
  )(time: Time, to: ActorRef, message: Any): Unit = {
    s ! ScheduledMessage(to, TimedMessage(time, message))
  }

  def props(stepTime: FiniteDuration): Props = Props(Scheduler(stepTime))
}
