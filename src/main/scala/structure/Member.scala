package structure

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.scalalogging.Logger
import structure.Scheduler.scheduleMessageWith
import structure.Timed.Time
import util.PaymentUtil.{Update, handlePayment}

case class Member(name: String, _assets: Map[Time, BigDecimal], scheduler: ActorRef)
    extends Actor {
  private var totalPaid: BigDecimal = 0
  private var currentTime = 0
  private val logger = Logger(name)
  private var assets = _assets

  override def receive: Receive = {
    case TimedMessage(t, m) =>
      assert(t >= currentTime, "Received message from the past.")
      currentTime = currentTime max t

      m match {
        case MarginCall(id, payment, maxDelay) =>
          val u = handlePayment(assets, payment, maxDelay)
          update(u)
          scheduleMessage(t + u.timedPayment.delay,
                          sender,
                          MarginCallResponse(self, id, u.timedPayment.payment))

        case DefaultFundCall(id, payment, maxDelay) =>
          val u = handlePayment(assets, payment, maxDelay)
          update(u)
          scheduleMessage(t + u.timedPayment.delay,
                          sender,
                          DefaultFundCallResponse(self, id, u.timedPayment.payment))

        case UnfundedDefaultFundCall(id, waterfallId, payment, maxDelay) =>
          val u = handlePayment(assets, payment, maxDelay)
          update(u)
          scheduleMessage(
            t + u.timedPayment.delay,
            sender,
            UnfundedDefaultFundCallResponse(self, id, waterfallId, u.timedPayment.payment))

        // TODO Defaulted and Paid
      }
  }

  private def update(u: Update) = {
    assets = u.assets
    totalPaid += u.timedPayment.payment
  }

  private def scheduleMessage = scheduleMessageWith(scheduler) _
}

object Member {
  def props(name: String, assets: Map[Time, BigDecimal], scheduler: ActorRef): Props = {
    Props(Member(name, assets, scheduler))
  }
}
