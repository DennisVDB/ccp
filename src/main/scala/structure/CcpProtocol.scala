package structure

import akka.actor.ActorRef
import structure.Timed.Time

/**
  * Created by dennis on 8/12/16.
  */
trait CcpProtocol

case class MarginCall(id: RequestId, payment: BigDecimal, maxDelay: Time) extends CcpProtocol

case class MarginCallResponse(counterParty: ActorRef, id: RequestId, payment: BigDecimal)
    extends CcpProtocol

case class DefaultFundCall(id: RequestId, payment: BigDecimal, maxDelay: Time) extends CcpProtocol

case class DefaultFundCallResponse(counterParty: ActorRef, id: RequestId, payment: BigDecimal)
    extends CcpProtocol

case class UnfundedDefaultFundCall(id: RequestId,
                                   waterfallId: RequestId,
                                   payment: BigDecimal,
                                   maxDelay: Time)
    extends CcpProtocol

case class UnfundedDefaultFundCallResponse(counterParty: ActorRef,
                                           id: RequestId,
                                           waterfallId: RequestId,
                                           payment: BigDecimal)
    extends CcpProtocol

case object Default extends CcpProtocol

case object Defaulted extends CcpProtocol

case object Paid extends CcpProtocol

case class Waterfall(member: ActorRef, loss: BigDecimal) extends CcpProtocol

case class CoverWithDefaulted(member: ActorRef, loss: BigDecimal) extends CcpProtocol

case class CoverWithSurvivors(member: ActorRef, loss: BigDecimal) extends CcpProtocol

case class CollectUnfundedFunds(member: ActorRef, loss: BigDecimal) extends CcpProtocol

case class CoverWithSurvivorsUnfunded(member: ActorRef, loss: BigDecimal, collected: BigDecimal)
    extends CcpProtocol

case class CoverWithFirstLevelEquity(member: ActorRef, loss: BigDecimal) extends CcpProtocol

case class CoverWithSecondLevelEquity(member: ActorRef, loss: BigDecimal) extends CcpProtocol

case class WaterfallResult(lossLeft: Option[BigDecimal])

class RequestId(val id: String) extends AnyVal
