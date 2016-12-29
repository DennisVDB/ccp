package structure

import java.io.File

import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import akka.pattern.pipe
import com.github.tototoshi.csv.CSVWriter
import com.typesafe.scalalogging.Logger
import market.Portfolio
import market.Portfolio.{buyAll, sellAll}
import structure.Scheduler.scheduledMessage
import structure.Timed._
import structure.ccp._
import util.Result

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scalaz.Scalaz._
import scalaz._

/**
  * Describes a clearinghouse member. It responds to calls from CCPs using its assets to pay for it.
  * @param name name.
  * @param capital the assets owned with their respective liquidity.
  * @param scheduler message scheduler in order to send them in order.
  */
case class Member(name: String, capital: Portfolio, market: ActorRef, scheduler: ActorRef)
    extends Actor {
  private var totalPaid: BigDecimal = 0
  private var currentTime = zero
  private val logger = Logger(name)
  private val f = new File(s"$name.csv")
  private var _capital = Result.pure(capital)

  override def receive: Receive = LoggingReceive {
    case Paid => sender ! totalPaid

    case TimedMessage(t, m) =>
      assert(t >= currentTime, "Received message from the past.")
      currentTime = currentTime max t

      val currentCapital = _capital

      val origSender = sender

      m match {
        case Transfer(payment) =>
          require(payment >= 0)

          val newCapital = for {
            c <- currentCapital
            (newC, _) <- buyAll(c)(payment, t)
          } yield newC

          // Reinvestment not fail
          _capital = newCapital

        case MarginCall(id, payment, maxDelay) =>
          require(payment >= 0)

          writeToCsv(t, payment)

          val newCapitalAndTimeF = for {
            c <- currentCapital
            (newCap, timeToSell) <- sellAll(c)(payment, t)
            if timeToSell <= maxDelay
          } yield (newCap, timeToSell)

          val newCapitalF = newCapitalAndTimeF.map(_._1)
          val timeToSellF = newCapitalAndTimeF.map(_._2)

          // Fails if not enough was sold or on time, reassign same capital
          _capital = newCapitalF ||| currentCapital

          val response = ((timeToSellF | Timed.zero) |@| (timeToSellF.map(_ => payment) | 0))(
            (timeToSell, raisedAmount) => {
              scheduledMessage(t + timeToSell,
                               origSender,
                               MarginCallResponse(id, self, raisedAmount))
            })

          response pipeTo scheduler

        case DefaultFundCall(id, payment, maxDelay) =>
          require(payment >= 0)

          writeToCsv(t, payment)

          val newCapitalAndTimeF = for {
            c <- currentCapital
            (newCap, timeToSell) <- sellAll(c)(payment, t)
            if timeToSell <= maxDelay
          } yield (newCap, timeToSell)

          val newCapitalF = newCapitalAndTimeF.map(_._1)
          val timeToSellF = newCapitalAndTimeF.map(_._2)

          // Fails if not enough was sold or on time, reassign same capital
          _capital = newCapitalF ||| currentCapital

          val response = ((timeToSellF | Timed.zero) |@| (timeToSellF.map(_ => payment) | 0))(
            (timeToSell, raisedAmount) => {
              scheduledMessage(t + timeToSell,
                               origSender,
                               DefaultFundCallResponse(id, self, raisedAmount))
            })

          response pipeTo scheduler

        case UnfundedDefaultFundCall(id, waterfallId, payment, maxDelay) =>
          require(payment >= 0)

          writeToCsv(t, payment)

          val newCapitalAndTimeF = for {
            c <- currentCapital
            (newCap, timeToSell) <- sellAll(c)(payment, t)
            if timeToSell <= maxDelay
          } yield (newCap, timeToSell)

          val newCapitalF = newCapitalAndTimeF.map(_._1)
          val timeToSellF = newCapitalAndTimeF.map(_._2)

          // Fails if not enough was sold or on time, reassign same capital
          _capital = newCapitalF ||| currentCapital

          val response = ((timeToSellF | Timed.zero) |@| (timeToSellF
            .map(_ => payment) | 0))((timeToSell, raisedAmount) => {
            scheduledMessage(t + timeToSell,
                             origSender,
                             UnfundedDefaultFundCallResponse(id, waterfallId, self, raisedAmount))
          })

          response pipeTo scheduler
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
  def props[A](name: String, capital: Portfolio, market: ActorRef, scheduler: ActorRef): Props = {
    Props(Member(name, capital, market, scheduler))
  }
}
