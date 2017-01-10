package structure

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.scalalogging.Logger
import structure.Scenario.Done
import structure.Scheduler.{Release, Run, ScheduledMessage, Setup}
import structure.Timed._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

/**
  * Schedules messages so they are sent in the right order.
  * The simulation has an internal time system that is independent of the computation time and
  * order the messages are sent. All the timed messages are sent to the scheduler, it then sends the messages out so
  * that they are received in a chronological order depending on the simulation time.
  *
  * @param stepTime duration between the sending of eligible messages.
  */
case class Scheduler(stepTime: Time) extends Actor {

  // Set of messages to be sent at each time t.
  private val messages =
    new mutable.HashMap[Time, mutable.Set[(ActorRef, ScheduledMessage)]]
    with mutable.MultiMap[Time, (ActorRef, ScheduledMessage)]

  private val logger = Logger("Scheduler")

  private var time: Time = zero

  def receive: Receive = {
    case Setup(sendTo, ms: List[ScheduledMessage]) =>
      messages.clear()
      time = zero

      ms.foreach {
        case m @ ScheduledMessage(_, TimedMessage(t, _)) =>
          messages.addBinding(t, sender -> m)
      }

      sender ! Done

    case Run =>
      // Schedule next message release.
      context.system.scheduler.schedule(stepTime, stepTime)(self ! Release)

    case Release => if (messages.nonEmpty) release()

    case m @ ScheduledMessage(_, TimedMessage(t, _)) =>
      // Store the message for further release.
      messages.addBinding(t, sender -> m)
  }

  /**
    * Sends the messages due, in order of the arrival time.
    * The eligible messages sent are ordered so that messages left behind at the previous release still
    * arrive in the right order.
    */
  def release(): Unit = {
//    logger.debug(s"$time")
    val toSend =
      messages
        .filterKeys(_ <= time)
        .toSeq
        .sortBy(_._1) // Order by time
        .flatMap(_._2) // Select messages to send

    // Send eligible messages.
    toSend.foreach(sm => {
      val sender = sm._1
      val scheduledMessage = sm._2

      // Register the original sender as the sender, so the scheduler is transparent.
      scheduledMessage.to.tell(scheduledMessage.message, sender)
    })

//    messages -= time

    (zero.toUnit(res) to (time.toUnit(res), 1)).foreach(t =>
      messages -= FiniteDuration(t.toLong, res)) // Remove sent messages

    time += tick
  }
}

object Scheduler {
  case class Setup(sendTo: ActorRef, ms: List[ScheduledMessage])
  case object Run
  case object Release
  case class ScheduledMessage(to: ActorRef, message: TimedMessage)

  /**
    * Helper function for constructing a scheduled message.
    * @param time time to send the message.
    * @param to recipient of the message.
    * @param message message.
    * @return the message wrapped in a scheduled message.
    */
  def scheduledMessage(time: Time,
                       to: ActorRef,
                       message: Any): ScheduledMessage =
    ScheduledMessage(to, TimedMessage(time, message))

  def props(stepTime: FiniteDuration): Props = Props(Scheduler(stepTime))
}
