package structure

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.scalalogging.Logger
import structure.Scheduler.ScheduledMessage

import scala.collection.mutable

case class Member(name: String,
                  assets: mutable.LinkedHashMap[Long, BigDecimal],
                  scheduler: ActorRef)
    extends Actor {
  private var totalPaid: BigDecimal = 0
  private var currentTime = 0L
  private val logger = Logger(name)

  override def receive: Receive = {
    case TimedMessage(t, m) =>
      assert(t >= currentTime, "Received message from the past.")
      currentTime = currentTime max t

      m match {
        case MarginCall(id, payment, maxDelay) =>
          val tp = handlePayment(payment, maxDelay)
          val m = MarginCallResponse(self, id, tp.payment)
          scheduler ! ScheduledMessage(sender, TimedMessage(t + tp.delay, m))

        case DefaultFundCall(id, payment, maxDelay) =>
          val tp = handlePayment(payment, maxDelay)
          val m = DefaultFundCallResponse(self, id, tp.payment)
          scheduler ! ScheduledMessage(sender, TimedMessage(t + tp.delay, m))

        case UnfundedDefaultFundCall(id, waterfallId, payment, maxDelay) =>
          val tp = handlePayment(payment, maxDelay)
          val m =
            UnfundedDefaultFundCallResponse(self, id, waterfallId, tp.payment)
          scheduler ! ScheduledMessage(sender, TimedMessage(t + tp.delay, m))

        // TODO Defaulted and Paid
      }
  }

  private def handlePayment(payment: BigDecimal,
                            maxDelay: Long): TimedPayment = {
    var paymentLeft = payment
    var delay = 0L

    (0 to maxDelay).foreach(liquidity => {
      for {
        available <- assets.get(liquidity)
        payment = available min paymentLeft
      } yield {
        assets += (liquidity -> (available - payment))
        paymentLeft -= payment
        if (payment > 0) delay = liquidity
      }
    })

    val paid = payment - paymentLeft

    totalPaid += paid

    TimedPayment(paid, delay)
  }
}

object Member {
  def props(
      name: String,
      assets: Map[Long, BigDecimal],
      scheduler: ActorRef
  ): Props = {
    Props(
      Member(name,
             mutable.LinkedHashMap(assets.toSeq.sortBy(_._1): _*),
             scheduler))
  }
}
