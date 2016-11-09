import CcpProtocol._
import akka.actor.{Actor, Props}
import akka.event.LoggingReceive
import com.typesafe.scalalogging.Logger

case class Member(
    name: String,
    portfolio: Portfolio[Security],
    shouldDefault: Boolean
) extends Actor {
  private val logger = Logger(name)

  override def receive: Receive =
    if (shouldDefault) defaulted else notDefaulted

  def notDefaulted: Receive = LoggingReceive {
    case MarginCall(id, payment) =>
      totalPaid += updatePaid(payment)
      sender ! MarginCallResponse(self, id, payment)

    case DefaultFundCall(id, payment) =>
      totalPaid += updatePaid(payment)
      sender ! DefaultFundCallResponse(self, id, payment)

    case UnfundedDefaultFundCall(id, waterfallId, payment) =>
      totalPaid += updatePaid(payment)
      sender ! UnfundedDefaultFundCallResponse(self, id, waterfallId, payment)

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
      sender ! UnfundedDefaultFundCallResponse(self, id, waterfallId, payment min 0)

    case Defaulted => sender ! true

    case Paid => sender ! totalPaid
  }

  private var totalPaid: BigDecimal = 0

  private val updatePaid: BigDecimal => BigDecimal = _ max 0
}

object Member {
  def props(
      name: String,
      portfolio: Portfolio[Security],
      shouldDefault: Boolean = false
  ): Props = {
    Props(Member(name, portfolio, shouldDefault))
  }
}
