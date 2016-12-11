package structure

import structure.Timed.Time

/**
  * Created by dennis on 8/12/16.
  */
trait Timed
case class TimedMessage(time: Time, m: Any) extends Timed
case class TimedPayment(payment: BigDecimal, delay: Time) extends Timed

object Timed {
  type Time = Int
}
