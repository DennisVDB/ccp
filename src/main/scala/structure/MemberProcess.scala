package structure

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.Logger
import market.Portfolio
import market.Portfolio.{Transaction, _}
import structure.Scenario.{Movements, SendData}
import structure.Scheduler.scheduledMessage
import structure.Timed.Time
import structure.ccp._
import util.Result.Result

import scalaz.Scalaz._

/**
  * Created by dennis on 1/1/17.
  */
trait MemberProcess extends Actor {
  val name: String
  var _capital: Result[Portfolio]
  val scheduler: ActorRef
  val logger: Logger

  var movements: Map[Time, BigDecimal] = Map.empty

  def handleMemberTimedMessages(t: Time): Receive = {
    case SendData => sender ! Movements(name, movements)

    case Transfer(payment) =>
      if (payment < 0) logger.warn(s"Payment is $payment.")

//      movements |+|= Map(t -> payment)

      _capital = for {
        c <- _capital
        Transaction(newC, _, _) <- buyAll(c)(payment max 0, t)
//        _ = if (amount < payment)
//          logger.warn(
//            s"Could not invest everything. Invested $amount out of $payment.")
      } yield newC

    case MarginCall(rId, payment, maxDelay) =>
      require(payment >= 0, s"Payment to $name was $payment")
      handleMarginCall(rId, payment, maxDelay, t)

    case LossMarginCall(rId, payment, maxDelay) =>
      require(payment >= 0, s"Payment to $name was $payment")
      movements |+|= Map(t -> payment)
      handleMarginCall(rId, payment, maxDelay, t)

    case DefaultFundCall(rId, payment, maxDelay) =>
      require(payment >= 0)

      movements |+|= Map(t -> payment)

      val transaction = for {
        c <- _capital
        t <- sellAll(c)(payment, t)
          .ensure(s"Not sold on time for $name.") {
            case Transaction(_, _, t) => t <= maxDelay
          }
      } yield t

      val newCapitalF = transaction.map(_.portfolio)
      val timeToSellF = transaction.map(_.timeToPerform)
      val amountF = transaction.map(_.transactionAmount)

      // Fails if not enough was sold or on time, reassign same capital
      _capital = newCapitalF ||| _capital

      val timeToSell = timeToSellF | Timed.zero
      val amount = amountF | BigDecimal(0)

      scheduleMessage(t + timeToSell,
                      sender,
                      DefaultFundCallResponse(rId, self, amount))

    case UnfundedDefaultFundCall(rId, waterfallId, payment, maxDelay) =>
      require(payment >= 0)

      movements |+|= Map(t -> payment)

      val transaction = for {
        c <- _capital
        t <- sellAll(c)(payment, t)
          .ensure(s"Not sold on time for $name.") {
            case Transaction(_, _, time) => time <= maxDelay
          }
      } yield t

      val newCapitalF = transaction.map(_.portfolio)
      val timeToSellF = transaction.map(_.timeToPerform)
      val amountF = transaction.map(_.transactionAmount)

      // Fails if not enough was sold or on time, reassign same capital
      _capital = newCapitalF ||| _capital

      val timeToSell = timeToSellF | Timed.zero
      val amount = amountF | BigDecimal(0)

      scheduleMessage(
        t + timeToSell,
        sender,
        UnfundedDefaultFundCallResponse(rId, waterfallId, self, amount))

    case VMGHLoss(loss) => movements |+|= Map(t -> loss)
  }

  private def handleMarginCall(rId: RequestId, payment: BigDecimal, maxDelay: Time, t: Time) = {
    val transaction = for {
      c <- _capital
      t <- sellAll(c)(payment, t)
        .ensure(s"Not sold on time for $name.") {
          case Transaction(_, _, t) => t <= maxDelay
        }
    } yield t

    val newCapitalF = transaction.map(_.portfolio)
    val timeToSellF = transaction.map(_.timeToPerform)
    val amountF = transaction.map(_.transactionAmount)

    // Fails if not enough was sold or on time, reassign same capital
    _capital = newCapitalF ||| _capital

    val timeToSell = timeToSellF | Timed.zero
    val amount = amountF | BigDecimal(0)

    scheduleMessage(t + timeToSell,
      sender,
      MarginCallResponse(rId, self, amount))
  }

  /**
    * Sends the message to the scheduler.
    * @param time time to deliver the message.
    * @param to recipient of the message.
    * @param message the message.
    */
  def scheduleMessage(time: Time, to: ActorRef, message: Any) =
    scheduler ! scheduledMessage(time, to, message)
}
