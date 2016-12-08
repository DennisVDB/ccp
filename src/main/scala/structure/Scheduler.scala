package structure

import akka.actor.{Actor, ActorRef}
import structure.Scheduler.{Init, Release, ScheduledMessage}

import scala.collection.mutable
import scala.concurrent.duration._

/**
  * Created by dennis on 8/12/16.
  */
case class Scheduler(stepTime: Long) extends Actor {
  val messages = new mutable.HashMap[Long, Set[ScheduledMessage]]
  with mutable.MultiMap[Long, ScheduledMessage]

  var time: Long = 0L

  def receive: Receive = {
    case Init =>
      context.system.scheduler.scheduleOnce(100 milliseconds)(self ! Release)

    case Release =>
      release()
      context.system.scheduler.scheduleOnce(100 milliseconds)(self ! Release)

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
  case object Init
  case object Release
  case class ScheduledMessage(receiver: ActorRef, message: TimedMessage)
}
