package structure

/**
  * Created by dennis on 8/12/16.
  */
case class TimedMessage(time: Long, m: Any)
case class TimedPayment(payment: BigDecimal, delay: Long)
