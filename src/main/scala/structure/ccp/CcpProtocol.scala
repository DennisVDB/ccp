package structure.ccp

import akka.actor.ActorRef
import structure.Timed.Time

/**
  * Created by dennis on 8/12/16.
  */
trait CcpProtocol

case class MarginCall(id: RequestId, payment: BigDecimal, maxDelay: Time) extends CcpProtocol

case class MarginCallResponse(id: RequestId, counterParty: ActorRef, payment: BigDecimal)
    extends CcpProtocol

case class DefaultFundCall(id: RequestId, payment: BigDecimal, maxDelay: Time) extends CcpProtocol

case class DefaultFundCallResponse(id: RequestId, counterParty: ActorRef, payment: BigDecimal)
    extends CcpProtocol

case class UnfundedDefaultFundCall(id: RequestId,
                                   waterfallId: WaterfallId,
                                   payment: BigDecimal,
                                   maxDelay: Time)
    extends CcpProtocol

case class UnfundedDefaultFundCallResponse(id: RequestId, waterfallId: WaterfallId, counterParty: ActorRef, payment: BigDecimal)
    extends CcpProtocol

case class Transfer(payment: BigDecimal) extends CcpProtocol

case class FailedStage(stage: Waterfall.WaterfallStage) extends CcpProtocol

case object Paid extends CcpProtocol

class RequestId(val id: String) extends AnyVal
class WaterfallId(val id: String) extends AnyVal
