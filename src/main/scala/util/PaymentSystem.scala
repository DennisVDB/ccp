package util

import cats.data.State
import cats.implicits._
import structure.Timed._
import structure.TimedPayment

import scala.concurrent.duration.FiniteDuration

/**
  * Payment system
  */
trait PaymentSystem {

  /**
    * Sells the assets in the allotted time in order to
    * get as much as possible for the payment, as quickly as possible.
    * @param assets the assets up for selling.
    * @param payment the amount that needs to be collected.
    * @param maxDelay the alloted time.
    * @return
    */
  def handlePayment(assets: Map[Time, BigDecimal], payment: BigDecimal, maxDelay: Time): Update = {
    val s = for {
      _ <- (zero.toUnit(res) to (maxDelay.toUnit(res), 1))
        .map(t => FiniteDuration(t.toLong, res))
        .map(State.modify[WorkState] _ compose update) // List of state transitions
        .toList
        .sequence[State[WorkState, ?], Unit] // State after performing all the transitions

      s <- State.get
    } yield TimedPayment(payment - s.paymentLeft, s.currentDelay)

    val (ws, tp) = s.run(WorkState(assets, payment, zero)).value

    Update(ws.assets, tp)
  }

  /**
    * Defines the work based on what has been done previously.
    * @return Work state
    */
  private val update =
    (t: Time) =>
      (w: WorkState) => {
        val available = w.assets.getOrElse(t, BigDecimal(0))

        // Sell what is needed
        val sold = available min w.paymentLeft

        // Only update delay if something was actually sold.
        val currentDelay = if (sold > 0) t else w.currentDelay

        WorkState(w.assets + (t -> (available - sold)), w.paymentLeft - sold, currentDelay)
    }

  case class Update(assets: Map[Time, BigDecimal], timedPayment: TimedPayment)

  private case class WorkState(assets: Map[Time, BigDecimal],
                               paymentLeft: BigDecimal,
                               currentDelay: Time)
}
