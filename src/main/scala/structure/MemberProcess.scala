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
  val scheduler: ActorRef
  val logger: Logger
  val shouldDefault: Boolean

  var movements: Map[Time, BigDecimal]
  var _capital: Result[Portfolio]

  def handleMemberTimedMessages(t: Time): Receive = {
    case SendData =>
      sender ! Movements(name, movements)

    case Transfer(payment) =>
      if (payment < 0) logger.warn(s"Payment is $payment.")

      _capital = for {
        c <- _capital
        Transaction(newC, _, _) <- buyAll(c)(payment max 0, t)
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

      if (shouldDefault) {
        scheduleMessage(t, sender, DefaultFundCallResponse(rId, self, 0))
      } else {
//        scheduleMessage(t + FiniteDuration(15, MINUTES),
//                        sender,
//                        DefaultFundCallResponse(rId, self, payment))

//        movements |+|= Map((t + FiniteDuration(15, MINUTES)) -> payment)

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

        movements |+|= Map((t + timeToSell) -> amount)

        scheduleMessage(t + timeToSell,
                        sender,
                        DefaultFundCallResponse(rId, self, payment))
      }

    case UnfundedDefaultFundCall(rId, waterfallId, payment, maxDelay) =>
      require(payment >= 0)

      if (shouldDefault) {
        scheduleMessage(
          t,
          sender,
          UnfundedDefaultFundCallResponse(rId, waterfallId, self, 0))
      } else {
//        scheduleMessage(
//          t + FiniteDuration(15, MINUTES),
//          sender,
//          UnfundedDefaultFundCallResponse(rId, waterfallId, self, payment))
//
//        movements |+|= Map((t + FiniteDuration(15, MINUTES)) -> payment)

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

        movements |+|= Map((t + timeToSell) -> amount)

        scheduleMessage(
          t + timeToSell,
          sender,
          UnfundedDefaultFundCallResponse(rId, waterfallId, self, payment))
      }

    case VMGHLoss(loss) => movements |+|= Map(t -> loss)
  }

  private def handleMarginCall(rId: RequestId,
                               payment: BigDecimal,
                               maxDelay: Time,
                               t: Time) = {
    if (shouldDefault) {
      scheduleMessage(t, sender, MarginCallResponse(rId, self, 0))
    } else {
//      scheduleMessage(t + FiniteDuration(15, MINUTES),
//        sender,
//        MarginCallResponse(rId, self, payment))
//
//      movements |+|= Map((t + FiniteDuration(15, MINUTES)) -> payment)

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
                      MarginCallResponse(rId, self, payment))
    }
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
