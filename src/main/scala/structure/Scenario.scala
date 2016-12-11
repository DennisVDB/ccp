package structure

import akka.actor.{Actor, ActorRef, Props}
import market.Market
import structure.Scheduler.scheduleMessageWith
import structure.Timed.Time

/**
  * Created by dennis on 23/10/16.
  */
case class Scenario[A](ccps: List[ActorRef],
                       market: Market[A],
                       timeHorizon: Time,
                       scheduler: ActorRef)
    extends Actor {
  override def receive: Receive = {
    case Scenario.Run =>
      ccps.foreach(_ ! Ccp.Run(timeHorizon))
      scheduler ! Scheduler.Run
  }

  private def scheduleMessage = scheduleMessageWith(scheduler) _
}

object Scenario {
  def props[A](ccps: List[ActorRef],
               market: Market[A],
               timeHorizon: Time,
               scheduler: ActorRef): Props = {
    Props(Scenario(ccps, market, timeHorizon, scheduler))
  }

  case object Run
}
