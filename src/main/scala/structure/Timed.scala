package structure

import java.util.concurrent.TimeUnit

import structure.Timed.Time

import scala.concurrent.duration.FiniteDuration

/**
  * Timed version of messages and payments.
  */
trait Timed
case class TimedMessage(time: Time, m: Any) extends Timed
case class TimedPayment(payment: BigDecimal, delay: Time) extends Timed

object Timed {
  type Time = FiniteDuration
  val res = TimeUnit.MINUTES
  val tick = FiniteDuration(1, res)
  val zero = FiniteDuration(0, res)
}
