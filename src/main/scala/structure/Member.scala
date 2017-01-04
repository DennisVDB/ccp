package structure

import akka.Done
import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.scalalogging.Logger
import market.Portfolio
import structure.Scenario.Reset
import structure.Timed._
import util.Result
import util.Result.{Result, pure}

/**
  * Describes a clearinghouse member. It responds to calls from CCPs using its assets to pay for it.
  * @param name name.
  * @param capital the assets owned with their respective liquidity.
  * @param scheduler message scheduler in order to send them in order.
  */
case class Member(name: String, capital: Portfolio, scheduler: ActorRef)
    extends Actor
    with MemberProcess {
  val logger: Logger = Logger(name)
  var _capital: Result[Portfolio] = pure(capital)
  var currentTime: Time = zero

  def receive: Receive = {
    case Reset =>
      _capital = Result.pure(capital)
      movements = Map.empty
      currentTime = zero
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
  def props[A](name: String, capital: Portfolio, scheduler: ActorRef): Props = {
    Props(Member(name, capital, scheduler))
  }
}
