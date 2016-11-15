import CcpProtocol._
import akka.actor.{Actor, Props}
import akka.event.LoggingReceive
import com.typesafe.scalalogging.Logger

case class Member(
    name: String,
    portfolio: Portfolio[Security], // TODO remove
    _cash: BigDecimal,
    shouldDefault: Boolean
) extends Actor {
  private val logger = Logger(name)

  override def receive: Receive =
    if (shouldDefault) defaulted else notDefaulted

  def notDefaulted: Receive = LoggingReceive {
    case MarginCall(id, payment) =>
      sender ! MarginCallResponse(self, id, handlePayment(payment))

    case DefaultFundCall(id, payment) =>
      sender ! DefaultFundCallResponse(self, id, handlePayment(payment))

    case UnfundedDefaultFundCall(id, waterfallId, payment) =>
      sender ! UnfundedDefaultFundCallResponse(self,
                                               id,
                                               waterfallId,
                                               handlePayment(payment))

    case Default =>
      logger.debug(s"----- defaulted! -----")
      context become defaulted
    case Defaulted => sender ! false

    case Paid => sender ! totalPaid
  }

  def defaulted: Receive = LoggingReceive {
    case MarginCall(id, payment) =>
      sender ! MarginCallResponse(self, id, payment min 0)

    case DefaultFundCall(id, payment) =>
      sender ! DefaultFundCallResponse(self, id, payment min 0)

    case UnfundedDefaultFundCall(id, waterfallId, payment) =>
      sender ! UnfundedDefaultFundCallResponse(self,
                                               id,
                                               waterfallId,
                                               payment min 0)

    case Defaulted => sender ! true

    case Paid => sender ! totalPaid
  }

  private var totalPaid: BigDecimal = 0

  private val updatePaid: BigDecimal => BigDecimal = _ max 0

  private var cash = _cash

  def handlePayment(payment: BigDecimal): BigDecimal = {
    if (payment >= 0) {
      val paying = payment min cash
      totalPaid += paying
      cash -= paying

      if (paying < payment) context become defaulted

      paying
    } else payment
  }
}

object Member {
  def props(
      name: String,
      portfolio: Portfolio[Security],
      cash: BigDecimal = BigDecimal(Double.MaxValue),
      shouldDefault: Boolean = false
  ): Props = {
    Props(Member(name, portfolio, cash, shouldDefault))
  }
}
