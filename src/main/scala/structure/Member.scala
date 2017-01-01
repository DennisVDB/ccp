package structure

import java.io.File

import akka.actor.{Actor, ActorRef, Props}
import com.github.tototoshi.csv.CSVWriter
import com.typesafe.scalalogging.Logger
import market.Portfolio
import market.Portfolio.{Transaction, buyAll, sellAll}
import structure.Scheduler.scheduledMessage
import structure.Timed._
import structure.ccp._
import util.DataUtil.ec
import util.Result

import scala.concurrent.Future

/**
  * Describes a clearinghouse member. It responds to calls from CCPs using its assets to pay for it.
  * @param name name.
  * @param capital the assets owned with their respective liquidity.
  * @param scheduler message scheduler in order to send them in order.
  */
case class Member(name: String, capital: Portfolio, scheduler: ActorRef)
    extends Actor {
  private var totalPaid: BigDecimal = 0
  private var currentTime = zero
  private val logger = Logger(name)
  private val f = new File(s"$name.csv")
  private var _capital = Result.pure(capital)

  override def receive: Receive = {
    case Paid => sender ! totalPaid

    case TimedMessage(t, m) =>
      assert(t >= currentTime, "Received message from the past.")
      currentTime = currentTime max t

      val currentCapital = _capital

      m match {
        case Transfer(payment) =>
          require(payment >= 0)

          _capital = for {
            c <- currentCapital
            Transaction(newC, amount, _) <- buyAll(c)(payment, t)
            _ = if (amount < payment)
              logger.warn(
                s"Could not invest everything. Invested $amount out of $payment.")
          } yield newC

        case MarginCall(rId, payment, maxDelay) =>
          require(payment >= 0, s"Payment to $name was $payment")

          writeToCsv(t, payment)

          val transaction = for {
            c <- currentCapital
            t <- sellAll(c)(payment, t)
              .ensure(s"Not sold on time for $name.") {
                case Transaction(_, _, t) => t <= maxDelay
              }
          } yield t

          val newCapitalF = transaction.map(_.portfolio)
          val timeToSellF = transaction.map(_.timeToPerform)
          val amountF = transaction.map(_.transactionAmount)

          // Fails if not enough was sold or on time, reassign same capital
          _capital = newCapitalF ||| currentCapital

          val timeToSell = timeToSellF | Timed.zero
          val amount = amountF | BigDecimal(0)

          scheduleMessage(t + timeToSell,
                          sender,
                          MarginCallResponse(rId, self, amount))

        case DefaultFundCall(rId, payment, maxDelay) =>
          require(payment >= 0)

          writeToCsv(t, payment)

          val transaction = for {
            c <- currentCapital
            t <- sellAll(c)(payment, t)
              .ensure(s"Not sold on time for $name.") {
                case Transaction(_, _, t) => t <= maxDelay
              }
          } yield t

          val newCapitalF = transaction.map(_.portfolio)
          val timeToSellF = transaction.map(_.timeToPerform)
          val amountF = transaction.map(_.transactionAmount)

          // Fails if not enough was sold or on time, reassign same capital
          _capital = newCapitalF ||| currentCapital

          val timeToSell = timeToSellF | Timed.zero
          val amount = amountF | BigDecimal(0)

          scheduleMessage(t + timeToSell,
                          sender,
                          DefaultFundCallResponse(rId, self, amount))

        case UnfundedDefaultFundCall(rId, waterfallId, payment, maxDelay) =>
          require(payment >= 0)

          writeToCsv(t, payment)

          val transaction = for {
            c <- currentCapital
            t <- sellAll(c)(payment, t)
              .ensure(s"Not sold on time for $name.") {
                case Transaction(_, _, time) => time <= maxDelay
              }
          } yield t

          val newCapitalF = transaction.map(_.portfolio)
          val timeToSellF = transaction.map(_.timeToPerform)
          val amountF = transaction.map(_.transactionAmount)

          // Fails if not enough was sold or on time, reassign same capital
          _capital = newCapitalF ||| currentCapital

          val timeToSell = timeToSellF | Timed.zero
          val amount = amountF | BigDecimal(0)

          scheduleMessage(
            t + timeToSell,
            sender,
            UnfundedDefaultFundCallResponse(rId, waterfallId, self, amount))
      }
  }

  private def writeToCsv(t: Time, payment: BigDecimal): Unit = {
    Future {
      val writer = CSVWriter.open(f, append = true)
      val stringifiedTime = t.toUnit(res).toString
      val stringifiedPayment = payment //assets.values.sum.toString
      val row = List(stringifiedTime, stringifiedPayment)
      writer.writeRow(row)
      writer.close()
    }
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

object Member {
  def props[A](name: String, capital: Portfolio, scheduler: ActorRef): Props = {
    Props(Member(name, capital, scheduler))
  }
}
