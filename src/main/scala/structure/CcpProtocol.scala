import akka.actor.ActorRef

/**
  * Created by dennis on 16/10/16.
  */
object CcpProtocol {
  sealed trait Message

  case class MarginCall(id: RequestId, payment: BigDecimal) extends Message

  case class MarginCallResponse(counterParty: ActorRef, id: RequestId, payment: BigDecimal)
      extends Message

  case class DefaultFundCall(id: RequestId, payment: BigDecimal) extends Message

  case class DefaultFundCallResponse(counterParty: ActorRef, id: RequestId, payment: BigDecimal)
      extends Message

  case class UnfundedDefaultFundCall(id: RequestId, waterfallId: RequestId, payment: BigDecimal)
      extends Message

  case class UnfundedDefaultFundCallResponse(counterParty: ActorRef,
                                             id: RequestId,
                                             waterfallId: RequestId,
                                             payment: BigDecimal)
      extends Message

  case object Default extends Message

  case object Defaulted extends Message

  case object Paid extends Message

  case class Waterfall(member: ActorRef, cost: BigDecimal) extends Message

  case class CoverWithDefaultingMember(member: ActorRef, cost: BigDecimal) extends Message

  case class CoverWithNonDefaultingMembers(member: ActorRef, cost: BigDecimal) extends Message

  case class CollectUnfundedFunds(member: ActorRef, cost: BigDecimal) extends Message

  case class CoverWithNonDefaultingUnfundedFunds(member: ActorRef,
                                                 cost: BigDecimal,
                                                 collected: BigDecimal)
      extends Message

  case class CoverWithFirstLevelEquity(member: ActorRef, cost: BigDecimal) extends Message

  case class CoverWithSecondLevelEquity(member: ActorRef, cost: BigDecimal) extends Message

  case class WaterfallResult(costLeft: Option[BigDecimal])

  class RequestId(val id: String) extends AnyVal

}
