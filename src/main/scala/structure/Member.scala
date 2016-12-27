package structure

import java.io.File

import akka.actor.{Actor, ActorRef, Props}
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
import scalaz.std.scalaFuture._

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
  private var _capital = Future.successful(capital)

  override def receive: Receive = {
    case Paid => sender ! totalPaid

    case TimedMessage(t, m) =>
      assert(t >= currentTime, "Received message from the past.")
      currentTime = currentTime max t

      m match {
        case MarginCall(id, payment, maxDelay) =>
          val origSender = sender

          writeToCsv(t, payment)

          if (payment < 0) {
            // Always answer.
            scheduleMessage(t, origSender, MarginCallResponse(id, self, payment))

            val newCapital = for {
              c <- Result.fromFuture(_capital)
              (newC, _) <- buyAll(c)(payment, t)
            } yield newC

            // Reinvestment not fail
            _capital = newCapital | (throw new IllegalStateException())
          } else {
            val newCapitalAndTimeF = for {
              c <- Result.fromFuture(_capital)
              (newCap, timeToSell) <- sellAll(c)(payment, t)
              if timeToSell <= maxDelay
            } yield (newCap, timeToSell)

            val newCapitalF = newCapitalAndTimeF.map(_._1)
            val timeToSellF = newCapitalAndTimeF.map(_._2)

            // Fails if not enough was sold or on time, reassign same capital
            _capital = newCapitalF getOrElseF _capital

            val responseMessage = for {
              timeToSell <- timeToSellF
            } yield
              scheduledMessage(t + timeToSell, origSender, MarginCallResponse(id, self, payment))

            val defaultResponseMessage =
              scheduledMessage(t, origSender, MarginCallResponse(id, self, 0))

            // Fails if not enough was sold or on time, sends 0.
            responseMessage | defaultResponseMessage pipeTo scheduler
          }

        case DefaultFundCall(id, payment, maxDelay) =>
          val origSender = sender

          writeToCsv(t, payment)

          val newCapitalAndTimeF = for {
            c <- Result.fromFuture(_capital)
            (newCap, timeToSell) <- sellAll(c)(payment, t)
            if timeToSell <= maxDelay
          } yield (newCap, timeToSell)

          val newCapitalF = newCapitalAndTimeF.map(_._1)
          val timeToSellF = newCapitalAndTimeF.map(_._2)

          // Fails if not enough was sold or on time, reassign same capital
          _capital = newCapitalF getOrElseF _capital

          val responseMessage = for {
            timeToSell <- timeToSellF
          } yield
            scheduledMessage(t + timeToSell,
                             origSender,
                             DefaultFundCallResponse(id, self, payment))

          val defaultResponseMessage =
            scheduledMessage(t, origSender, DefaultFundCallResponse(id, self, 0))

          // Fails if not enough was sold or on time, sends 0.
          responseMessage | defaultResponseMessage pipeTo scheduler

        case UnfundedDefaultFundCall(id, waterfallId, payment, maxDelay) =>
          val origSender = sender

          writeToCsv(t, payment)

          val newCapitalAndTimeF = for {
            c <- Result.fromFuture(_capital)
            (newCap, timeToSell) <- sellAll(c)(payment, t)
            if timeToSell <= maxDelay
          } yield (newCap, timeToSell)

          val newCapitalF = newCapitalAndTimeF.map(_._1)
          val timeToSellF = newCapitalAndTimeF.map(_._2)

          // Fails if not enough was sold or on time, reassign same capital
          _capital = newCapitalF getOrElseF _capital

          val responseMessage = for {
            timeToSell <- timeToSellF
          } yield
            scheduledMessage(t + timeToSell,
                             origSender,
                             UnfundedDefaultFundCallResponse(id, waterfallId, self, payment))

          val defaultResponseMessage =
            scheduledMessage(t,
                             origSender,
                             UnfundedDefaultFundCallResponse(id, waterfallId, self, 0))

          // Fails if not enough was sold or on time, sends 0.
          responseMessage | defaultResponseMessage pipeTo scheduler
      }
  }

//  /**
//    * Updates the member with its new assets and what has been paid for the call.
//    * @param assets new assets.
//    * @param payment additional payment.
//    */
//  private def update(portfolio: Portfolio, payment: BigDecimal, t: Time): Unit = {
//    _assets = assets
//
//    writeToCsv(t, -payment)
//
//    totalPaid += payment
//  }

//  /**
//    * Updates the assets with the payment. Liquidity is assumed to be 0.
//    * @param payment payment to add.
//    */
//  private def updateAssets(payment: BigDecimal, t: Time): Unit = {
//    writeToCsv(t, payment)
//
//    for {
//      currentAmount <- _assets.get(zero)
//
//      _ = _assets += zero -> (currentAmount + payment)
//    } yield ()
//  }

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
