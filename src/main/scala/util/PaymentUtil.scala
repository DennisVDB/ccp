package util

import cats.data.State
import cats.implicits._
import structure.Timed.Time
import structure.TimedPayment

/**
  * Created by dennis on 10/12/16.
  */
object PaymentUtil {
  def handlePayment(assets: Map[Time, BigDecimal],
                    payment: BigDecimal,
                    maxDelay: Time): Update = {
    val s = for {
      _ <- (0 to maxDelay)
        .map(State.modify[Work] _ compose update)
        .toList
        .sequence[State[Work, ?], Unit]

      s <- State.get
    } yield TimedPayment(payment - s.paymentLeft, s.currentDelay)

    val t = s.run(Work(assets, payment, 0)).value

    Update(t._1.assets, t._2)
  }

  case class Update(assets: Map[Time, BigDecimal], timedPayment: TimedPayment)

  private def update =
    (l: Time) =>
      (w: Work) => {
        val available = w.assets.getOrElse(l, BigDecimal(0))
        val used = available min w.paymentLeft

        val currentDelay = if (used > 0) l else w.currentDelay

        Work(w.assets + (l -> (available - used)),
             w.paymentLeft - used,
             currentDelay)
    }

  private case class Input(payment: BigDecimal, liquidity: Time)
  private case class Work(assets: Map[Time, BigDecimal],
                          paymentLeft: BigDecimal,
                          currentDelay: Time)
}
