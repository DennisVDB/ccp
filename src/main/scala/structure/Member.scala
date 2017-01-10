package structure

import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import com.typesafe.scalalogging.Logger
import market.Portfolio
import structure.Member.Setup
import structure.Scenario.Done
import structure.Timed._
import util.Result
import util.Result.{Result, pure}

/**
  * Describes a clearinghouse member. It responds to calls from CCPs using its assets to pay for it.
  * @param name name.
  * @param capital the assets owned with their respective liquidity.
  * @param scheduler message scheduler in order to send them in order.
  */
case class Member(name: String,
                  capital: Portfolio,
                  scheduler: ActorRef,
                  _shouldDefault: Boolean)
    extends Actor
    with MemberProcess {
  var currentTime: Time = zero

  val logger: Logger = Logger(name)
  var _capital: Result[Portfolio] = pure(capital)
  var movements: Map[Time, BigDecimal] = Map.empty
  val shouldDefault: Boolean = _shouldDefault

  def receive: Receive = LoggingReceive {
    case Setup =>
      currentTime = zero

      _capital = Result.pure(capital)
      movements = Map.empty

      sender ! Done

    case TimedMessage(t, m) =>
      assert(
        t >= currentTime,
        "Received message from the past, time is +" + currentTime + ". " + m + " @" + t + " from " + sender)
      currentTime = currentTime max t

      handleMemberTimedMessages(t)(m)
  }
}

object Member {
  case object Setup

  def props[A](name: String,
               capital: Portfolio,
               scheduler: ActorRef,
               shouldDefault: Boolean = false): Props = {
    Props(Member(name, capital, scheduler, shouldDefault))
  }
}
