package structure

import akka.actor.{Actor, ActorRef, Props}
import structure.Scheduler.scheduledMessage
import structure.Timed.Time

/**
  * Creates a scenario in which clearinghouses and their members can run until the timeHorizon.
  * @param ccps the simulated CCPs.
  * @param timeHorizon the last moment in time a scheduled margin call can be made.
  * @param scheduler the scheduler for sending the messages in order.
  */
case class Scenario(ccps: Set[ActorRef], timeHorizon: Time, scheduler: ActorRef) extends Actor {
  override def receive: Receive = {
    case Scenario.Run =>
      // Starts the clearinghouses.
      ccps.foreach(_ ! Ccp.Run(timeHorizon))

      scheduler ! Scheduler.Run
  }

  /**
    * Sends the message to the scheduler.
    * @param time time to deliver the message.
    * @param to recipient of the message.
    * @param message the message.
    */
  private def scheduleMessage(time: Time, to: ActorRef, message: Any) =
    scheduler ! scheduledMessage(time, to, message)
}

object Scenario {
  def props[A](ccps: Set[ActorRef], timeHorizon: Time, scheduler: ActorRef): Props = {
    Props(Scenario(ccps, timeHorizon, scheduler))
  }

  case object Run
}
