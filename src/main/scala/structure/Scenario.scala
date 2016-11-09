import Ccp.TriggerMarginCalls
import akka.actor.{Actor, ActorRef, Props}

/**
  * Created by dennis on 23/10/16.
  */
case class Scenario[A](ccps: List[ActorRef], market: Market[A]) extends Actor {
  override def receive: Receive = {
    case TriggerMarginCalls =>
      market.shockAll(BigDecimal("-0.9"))
      ccps.head ! TriggerMarginCalls
//      ccps.foreach(_ ! TriggerMarginCalls)
//      for (i <- 0 to 100) {
//        ccps.foreach(ccp => ccp ! TriggerMarginCalls)
////        market.shockAll(r.nextGaussian())
//        market.shockAll(-Math.abs(r.nextGaussian()))
//      }
//      self ! TriggerMarginCalls
  }

  val r = scala.util.Random
}

object Scenario {
  def props[A](ccps: List[ActorRef], market: Market[A]): Props = {
    Props(Scenario(ccps, market))
  }
}
