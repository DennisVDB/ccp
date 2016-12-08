package structure

import akka.actor.ActorRef

/**
  * Created by dennis on 8/12/16.
  */
trait CcpProtocol

case class MarginCall(id: RequestId, payment: BigDecimal, maxDelay: Long) extends CcpProtocol

case class MarginCallResponse(counterParty: ActorRef,
                              id: RequestId,
                              payment: BigDecimal)
    extends CcpProtocol

case class DefaultFundCall(id: RequestId, payment: BigDecimal, maxDelay: Long)
    extends CcpProtocol

case class DefaultFundCallResponse(counterParty: ActorRef,
                                   id: RequestId,
                                   payment: BigDecimal)
    extends CcpProtocol

case class UnfundedDefaultFundCall(id: RequestId,
                                   waterfallId: RequestId,
                                   payment: BigDecimal, maxDelay: Long)
    extends CcpProtocol

case class UnfundedDefaultFundCallResponse(counterParty: ActorRef,
                                           id: RequestId,
                                           waterfallId: RequestId,
                                           payment: BigDecimal)
    extends CcpProtocol

case object Default extends CcpProtocol

case object Defaulted extends CcpProtocol

case object Paid extends CcpProtocol

case class Waterfall(member: ActorRef, cost: BigDecimal) extends CcpProtocol

case class CoverWithDefaultingMember(member: ActorRef, cost: BigDecimal)
    extends CcpProtocol

case class CoverWithNonDefaultingMembers(member: ActorRef, cost: BigDecimal)
    extends CcpProtocol

case class CollectUnfundedFunds(member: ActorRef, cost: BigDecimal)
    extends CcpProtocol

case class CoverWithNonDefaultingUnfundedFunds(member: ActorRef,
                                               cost: BigDecimal,
                                               collected: BigDecimal)
    extends CcpProtocol

case class CoverWithFirstLevelEquity(member: ActorRef, cost: BigDecimal)
    extends CcpProtocol

case class CoverWithSecondLevelEquity(member: ActorRef, cost: BigDecimal)
    extends CcpProtocol

case class WaterfallResult(costLeft: Option[BigDecimal])

class RequestId(val id: String) extends AnyVal
