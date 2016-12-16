package structure

import java.io.File

import akka.actor.{Actor, ActorRef, Props}
import com.github.tototoshi.csv.CSVWriter
import com.typesafe.scalalogging.Logger
import structure.Member.Delay
import structure.Scheduler.scheduledMessage
import structure.Timed._
import structure.ccp._
import util.PaymentSystem

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Describes a clearinghouse member. It responds to calls from CCPs using its assets to pay for it.
  * @param name name.
  * @param assets the assets owned with their respective liquidity.
  * @param delays delays for the computations.
  * @param scheduler message scheduler in order to send them in order.
  */
case class Member(name: String, assets: Map[Time, BigDecimal], delays: Delay, scheduler: ActorRef)
    extends Actor
    with PaymentSystem {
  private var totalPaid: BigDecimal = 0
  private var currentTime = zero
  private val logger = Logger(name)
  private val f = new File(s"$name.csv")
  private var _assets = assets

  override def receive: Receive = {
    case Paid => sender ! totalPaid

    case TimedMessage(t, m) =>
      assert(t >= currentTime, "Received message from the past.")
      currentTime = currentTime max t

      m match {
        case MarginCall(id, payment, maxDelay) =>
          if (payment < 0) {
            updateAssets(-payment, t)
            scheduleMessage(t, sender, MarginCallResponse(id, self, payment))
          }
          else {
            val u = handlePayment(_assets, payment, maxDelay - delays.callHandling)
            update(u.assets,
                   u.timedPayment.payment,
                   t + u.timedPayment.delay + delays.callHandling)
            scheduleMessage(t + u.timedPayment.delay + delays.callHandling,
                            sender,
                            MarginCallResponse(id, self, u.timedPayment.payment))
          }

        case DefaultFundCall(id, payment, maxDelay) =>
          val u = handlePayment(_assets, payment, maxDelay - delays.callHandling)
          update(u.assets, u.timedPayment.payment, t + u.timedPayment.delay + delays.callHandling)
          scheduleMessage(t + u.timedPayment.delay + delays.callHandling,
                          sender,
                          DefaultFundCallResponse(id, self, u.timedPayment.payment))

        case UnfundedDefaultFundCall(id, waterfallId, payment, maxDelay) =>
          val u = handlePayment(_assets, payment, maxDelay - delays.callHandling)
          update(u.assets, u.timedPayment.payment, t + u.timedPayment.delay + delays.callHandling)
          scheduleMessage(
            t + u.timedPayment.delay + delays.callHandling,
            sender,
            UnfundedDefaultFundCallResponse(waterfallId, id, self, u.timedPayment.payment))

        // TODO Defaulted and Paid
      }
  }

  /**
    * Updates the member with its new assets and what has been paid for the call.
    * @param assets new assets.
    * @param payment additional payment.
    */
  private def update(assets: Map[Time, BigDecimal], payment: BigDecimal, t: Time): Unit = {
    _assets = assets

    writeToCsv(t, -payment)

    totalPaid += payment
  }

  /**
    * Updates the assets with the payment. Liquidity is assumed to be 0.
    * @param payment payment to add.
    */
  private def updateAssets(payment: BigDecimal, t: Time): Unit = {
    writeToCsv(t, payment)

    for {
      currentAmount <- _assets.get(zero)

      _ = _assets += zero -> (currentAmount + payment)
    } yield ()
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
  case class Delay(callHandling: Time)

  def props(name: String,
            assets: Map[Time, BigDecimal],
            delays: Delay,
            scheduler: ActorRef): Props = {
    Props(Member(name, assets, delays, scheduler))
  }
}
