import java.util.UUID

import Ccp._
import CcpProtocol._
import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import akka.util.Timeout
import cats.data.Kleisli
import cats.instances.all._
import com.typesafe.scalalogging.Logger

import scala.collection._
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Models a central counterparty (CCP). Performs on demand margin calls and handles the subsequent
  * defaults through a cost waterfall.
  * @param name name of the CCP
  * @param memberPortfolios portfolios of each member it handles
  * @param ccpPortfolios portfolios of each ccp it handles
  * @param _equity equity owned by the CCP
  * @param rules rules for configuring the CCP
  * @param shouldDefault initial behavior in case of calls.
  * @tparam A type of the instruments the CCP handles.
  */
class Ccp[A](
    name: String,
    memberPortfolios: Map[ActorRef, Portfolio[A]],
    ccpPortfolios: => Map[ActorRef, Portfolio[A]],
    _equity: BigDecimal,
    rules: Rules,
    shouldDefault: Boolean
) extends Actor {
  override def receive: Receive =
    if (shouldDefault) defaulted else notDefaulted

  /**
    * Behavior when not defaulted.
    */
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

    //---
    case TriggerMarginCalls => triggerMarginCalls()

    case MarginCallResponse(responder, id, payment) =>
      handleMarginCallResponse(responder, id, payment)

    case DefaultFundCallResponse(responder, id, payment) =>
      handleDefaultFundCallResponse(responder, id, payment)

    case UnfundedDefaultFundCallResponse(responder,
                                         id,
                                         waterfallId,
                                         payment) =>
      handleUnfundedDefaultFundCallResponse(responder,
                                            id,
                                            waterfallId,
                                            payment)

    case Paid => sender ! totalPaid

    //---
    case Waterfall(member, cost) => coverWithDefaultedCollateral(member, cost)

    case CoverWithDefaultingMember(member, cost) =>
      coverWithDefaultedCollateral(member, cost)

    case CoverWithNonDefaultingMembers(member, cost) =>
      coverWithNonDefaultedCollateral(member, cost)

    case CollectUnfundedFunds(member, cost) =>
      collectUnfundedFunds(member, cost)

    case CoverWithNonDefaultingUnfundedFunds(member, cost, collected) =>
      coverWithNonDefaultingUnfundedFunds(member, cost, collected)

    case CoverWithFirstLevelEquity(member, cost) =>
      coverWithFirstLevelEquity(member, cost)

    case CoverWithSecondLevelEquity(member, cost) =>
      coverWithSecondLevelEquity(member, cost)

    case WaterfallResult(costLeft) => handleWaterfallResult(costLeft)
  }

  /**
    * Behavior when defaulted.
    */
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

  private val logger = Logger(name)

  /**
    * Total paid for other members default.
    */
  private var totalPaid: BigDecimal = 0

  private var equity: BigDecimal = _equity

  private val members: Set[ActorRef] = memberPortfolios.keys.toSet

  private val ccps: Set[ActorRef] = ccpPortfolios.keys.toSet

  private val allMembers: Set[ActorRef] = members ++ ccps

  private val allPortfolios: Map[ActorRef, Portfolio[A]] = memberPortfolios ++ ccpPortfolios

  /**
    * Set of members that have previously defaulted on their payments.
    */
  private var defaultedMembers: mutable.Set[ActorRef] =
    mutable.Set.empty[ActorRef]

  /**
    * Last prices of members' portfolios.
    */
  private val prices: mutable.Map[ActorRef, BigDecimal] = {
    val prices =
      for {
        (member, portfolio) <- allPortfolios
      } yield (member, portfolio.price)

    mutable.Map(prices.toSeq: _*).collect {
      case (key, Some(value)) => (key, value)
    }
  }

  /**
    * Posted margins of members.
    */
  private val margins: mutable.Map[ActorRef, BigDecimal] = {
    val margins =
      if (rules.ccpRules.participatesInMargin) {
        val memberMargins =
          for {
            (member, portfolio) <- memberPortfolios
          } yield (member, portfolio.margin(rules.memberRules.marginCoverage))

        val ccpMargins = for {
          (member, portfolio) <- ccpPortfolios
        } yield (member, portfolio.margin(rules.ccpRules.marginCoverage))

        memberMargins ++ ccpMargins
      } else {
        for {
          (member, portfolio) <- memberPortfolios
        } yield (member, portfolio.margin(rules.memberRules.marginCoverage))
      }

    logger.debug(margins.toString())

    mutable.Map(margins.toSeq: _*).collect {
      case (key, Some(value)) => (key, value)
    }
  }

  /**
    * Snapshot of the initial margins when setting up the CCP.
    */
  private val initialMargins: Map[ActorRef, BigDecimal] =
    margins.toMap

  /**
    * Posted default funds of members.
    */
  private val defaultFunds: mutable.Map[ActorRef, BigDecimal] = {
    val memberFunds = members.map {
      (_, Some(BigDecimal("20")))
    }

    val ccpFunds = ccps.map {
      (_, Some(BigDecimal("20")))
    }

    val funds = memberFunds ++ ccpFunds

    mutable.Map(funds.toSeq: _*).collect {
      case (key, Some(value)) => (key, value)
    }
  }

  /**
    * Snapshot of the default funds when setting up the CCP.
    */
  private val initDefaultFunds: Map[ActorRef, BigDecimal] = defaultFunds.toMap

  private val unfundedFundsLeft: mutable.Map[ActorRef, BigDecimal] = {
    allMembers.map(m => (m, rules.maximumFundCall))(breakOut)
  }

  /**
    * Computes the percentage of the default funds of the member ex defaulted members.
    *
    * @param member member for which to compute the pro-rata
    * @return percentage of the default funds of the member ex defaulted members.
    */
  private def proRataDefaultFunds(
      member: ActorRef
  ): Option[BigDecimal] = {
    val total =
      initDefaultFunds
        .withFilter(entry => !defaultedMembers.contains(entry._1))
        .map(_._2)
        .sum

    for {
      part <- defaultFunds.get(member)
    } yield {
      part / total
    }
  }

  /**
    * Computes the percentage of the initial margins of the member ex defaulted members.
    *
    * @param member member for which to compute the pro-rata
    * @return percentage of the initial margins of the member ex defaulted members.
    */
  private def proRataInitialMargins(
      member: ActorRef
  ): Option[BigDecimal] = {
    val total =
      initialMargins
        .withFilter(entry => !defaultedMembers.contains(entry._1))
        .map(_._2)
        .sum

    for {
      part <- margins.get(member)
    } yield part / total
  }

  /**
    * Expected margin payment after margin call.
    */
  private val expectedMarginPayments: mutable.Map[(ActorRef, RequestId),
                                                  BigDecimal] =
    mutable.Map.empty

  /**
    * Expected default fund payment after fund call.
    */
  private val expectedDefaultFundPayments: mutable.Map[(ActorRef, RequestId),
                                                       BigDecimal] =
    mutable.Map.empty

  private val expectedUnfundedFundsForWaterfall: mutable.Map[RequestId,
                                                             (BigDecimal,
                                                              Int,
                                                              ActorRef,
                                                              BigDecimal)] =
    mutable.Map.empty

  /**
    * Triggers needed margin calls.
    */
  private def triggerMarginCalls(): Unit = {
    implicit val timeout: Timeout = Timeout(60 seconds)

    allMembers.foreach(memberMarginCall)
  }

  /**
    * Performs margin call to member.
    *
    * @param member member
    */
  private def memberMarginCall(member: ActorRef): Unit = {
//    implicit val timeout: Timeout = Timeout(60 seconds)

    for {
      oldPrice <- prices.get(member)
      initialMargin <- initialMargins.get(member)

      currentPrice <- allPortfolios.get(member).flatMap(_.price)

      variationMargin = oldPrice - currentPrice

      margin <- margins.get(member)
      _ = margins.put(member, margin - variationMargin)

      // Amount below initial margin ...
      // (happens when previously
      // the variation margin was too small
      // to trigger a margin call)
      // ... plus the variation margin.
      marginCall = (initialMargin - margin) + variationMargin
      if marginCall.abs >= rules.minimumTransfer && marginCall != 0

      _ = prices.put(member, currentPrice)

      id = generateUuid
      _ = expectedMarginPayments.put((member, id), marginCall)
    } yield member ! MarginCall(id, marginCall)
  }

  /**
    * Handles the response to margin calls.
    *
    * @param member  member that replied
    * @param id      identifier of the request
    * @param payment payment received
    */
  private def handleMarginCallResponse(
      member: ActorRef,
      id: RequestId,
      payment: BigDecimal
  ): Unit = {
    // Update margin with payment
    for {
      currentMargin <- margins.get(member)

      expectedPayment <- expectedMarginPayments.get((member, id))

      replacementCost <- allPortfolios.get(member).flatMap(_.replacementCost)
    } yield {
      logger.debug(
        s"From $member (margin call) Expected $expectedPayment, received $payment"
      )

      margins.put(member, currentMargin + payment)

      expectedDefaultFundPayments.remove((member, id))

      managePotentialDefault(
        member,
        expectedPayment - payment,
        replacementCost,
        payment < expectedPayment
      )
    }
  }

  /**
    * Handles the response to default fund calls.
    *
    * @param member  member that replied
    * @param id      identifier of the request
    * @param payment payment received
    */
  private def handleDefaultFundCallResponse(
      member: ActorRef,
      id: RequestId,
      payment: BigDecimal
  ): Unit = {
    // Update margin with payment
    for {
      currentDefaultFund <- defaultFunds.get(member)
      expectedPayment <- expectedDefaultFundPayments.get((member, id))
      replacementCost <- allPortfolios.get(member).flatMap(_.replacementCost)
    } yield {
      logger.debug(
        s"From $member (fund call) Expected $expectedPayment, received $payment"
      )

      defaultFunds
        .put(member, currentDefaultFund + payment) // update fund w/ payment

      expectedDefaultFundPayments.remove((member, id))

      managePotentialDefault(
        member,
        expectedPayment - payment,
        replacementCost,
        payment < expectedPayment
      )
    }
  }

  /**
    * Handles the response to unfunded default fund calls.
    *
    * @param member  member that replied
    * @param id      identifier of the request
    * @param payment payment received
    */
  private def handleUnfundedDefaultFundCallResponse(
      member: ActorRef,
      id: RequestId,
      waterfallId: RequestId,
      payment: BigDecimal
  ): Unit = {
    for {
      expectedPayment <- expectedDefaultFundPayments.get((member, id))
      replacementCost <- allPortfolios.get(member).flatMap(_.replacementCost)
      (collected, waitingFor, defaultingMember, cost) <- expectedUnfundedFundsForWaterfall
        .get(waterfallId)

      _ = logger.debug(
        s"(collected = $collected, waitingFor = $waitingFor, member = $defaultingMember, cost = $cost")
    } yield {
      logger.debug(
        s"From $member (unfunded call) Expected $expectedPayment, received $payment"
      )

      expectedDefaultFundPayments.remove((member, id))

      if (waitingFor == 1) {
        expectedUnfundedFundsForWaterfall.remove(waterfallId)
        self ! CoverWithNonDefaultingUnfundedFunds(defaultingMember,
                                                   cost,
                                                   collected + payment)
      } else {
        logger.debug(s" PAYMENT $payment")

        expectedUnfundedFundsForWaterfall.put(
          waterfallId,
          (collected + payment, waitingFor - 1, defaultingMember, cost))
      }

      managePotentialDefault(
        member,
        expectedPayment - payment,
        replacementCost,
        payment < expectedPayment
      )
    }
  }

  /**
    * Checks if defaulted and then performs the default according to how much is owed.
    *
    * @param member          member that may have defaulted
    * @param cost            cost left after call
    * @param replacementCost replacement cost of the member's portfolio
    * @param defaulted       defaulted or not
    */
  private def managePotentialDefault(
      member: ActorRef,
      cost: BigDecimal,
      replacementCost: BigDecimal,
      defaulted: Boolean
  ): Unit = {
    if (defaulted) {
      if (defaultedMembers.contains(member)) {
        self ! Waterfall(member, cost)
      } else {
        defaultedMembers += member
        self ! Waterfall(member, cost + replacementCost)
      }
    }
  }

  private def handleWaterfallResult(costLeft: Option[BigDecimal]): Unit = {
    costLeft match {
      case Some(c) => updateDefaulted(c)
      case None => logger.debug("WATERFALL ERROR!")
    }
  }

  /**
    * Covers the cost with the defaulting member' posted initial margin.
    *
    * @return cost left after coverage
    */
  private def coverWithInitialMargin(defaultingMember: ActorRef) =
    Kleisli[Option, BigDecimal, BigDecimal](cost => {
      if (cost <= 0) {
        logger.debug(s"coverWithInitialMargin for $defaultingMember 0")
        Some(0)
      } else {
        // Left after using margin
        val costAfterMarginUse = for {
          margin <- margins.get(defaultingMember)
          costLeft = cost - margin
        } yield {
          updateAfterCost(margins)(defaultingMember, costLeft)
          costLeft
        }

        logger.debug(
          s"coverWithInitialMargin for $defaultingMember ${costAfterMarginUse
            .map(_ max 0)}")

        costAfterMarginUse.map(_ max 0)
      }
    })

  /**
    * Covers the cost with the defaulting member' posted default fund.
    *
    * @return cost left after coverage
    */
  private def coverWithFund(defaultingMember: ActorRef) =
    Kleisli[Option, BigDecimal, BigDecimal](cost => {
      if (cost <= 0) {
        logger.debug(s"coverWithFund for $defaultingMember 0}")

        Some(0)
      } else {
        // Left after funds use
        val costAfterDefaultFundUse = {
          for {
            defaultFund <- defaultFunds.get(defaultingMember)
            costLeft = cost - defaultFund
          } yield {
            updateAfterCost(defaultFunds)(defaultingMember, costLeft)
            costLeft
          }
        }

        logger.debug(
          s"coverWithFund for $defaultingMember ${costAfterDefaultFundUse.map(_ max 0)}")

        costAfterDefaultFundUse.map(_ max 0)
      }
    })

  /**
    * Default coverage waterfall part of the defaulting member.
    * @return cost after coverage
    */
  private def coverWithDefaultedCollateral(defaultingMember: ActorRef,
                                           cost: BigDecimal): Unit = {
    val f = coverWithInitialMargin(defaultingMember) andThen coverWithFund(
        defaultingMember)

    f(cost) match {
      case Some(costLeft) =>
        self ! CoverWithFirstLevelEquity(defaultingMember, costLeft)
      case None => self ! WaterfallResult(None)
    }
  }

  /**
    * Covers the cost with the non defaulting members' initial margins.
    * May not be used in case the rules say so.
    *
    * @return cost left after coverage
    */
  private def coverWithNonDefaultingInitialMargin(defaultingMember: ActorRef) =
    (cost: BigDecimal) => {
      if (cost <= 0) {
        logger.debug(
          s"coverWithNonDefaultingInitialMargin for $defaultingMember 0")

        BigDecimal(0)
      } else if (rules.marginIsRingFenced) cost
      else {
        val totalInitialMargins =
          margins
            .withFilter(entry => !defaultedMembers.contains(entry._1))
            .map(_._2)
            .sum

        allMembers
          .withFilter(!defaultedMembers.contains(_))
          .foreach(
            member => {
              for {
                currentMargin <- margins.get(member)
                proRata <- proRataInitialMargins(member)
                payment = (cost min totalInitialMargins) * proRata
                if payment != 0
              } yield {
                val id = generateUuid

                logger.debug(s"-- proRata $proRata and payment $payment")

                updateInitialMargin(member, currentMargin - payment)
                updateExpectedMarginPayments(member, id, payment)

                member ! MarginCall(id, payment)
              }
            }
          )

        logger.debug(
          s"coverWithNonDefaultingInitialMargin for $defaultingMember ${(cost - totalInitialMargins) max 0}")

        (cost - totalInitialMargins) max 0
      }
    }

  /**
    * Covers the cost with the non defaulting members' default funds.
    *
    * @return cost after coverage
    */
  private def coverWithNonDefaultingFunds(defaultingMember: ActorRef) =
    (cost: BigDecimal) => {
      if (cost <= 0) {
        logger.debug(s"coverWithNonDefaultingFunds for $defaultingMember 0")
        BigDecimal(0)
      } else {
        val totalDefaultFunds =
          defaultFunds
            .withFilter(entry => !defaultedMembers.contains(entry._1))
            .map(_._2)
            .sum

        allMembers
          .withFilter(!defaultedMembers.contains(_))
          .foreach(
            member => {
              for {
                currentMargin <- defaultFunds.get(member)
                proRata <- proRataDefaultFunds(member)
                fundPayment = (cost min totalDefaultFunds) * proRata
                if fundPayment != 0
              } yield {
                val id = generateUuid

                updateDefaultFund(member, currentMargin - fundPayment)
                updateExpectedFundPayments(member, id, fundPayment)

                member ! DefaultFundCall(id, fundPayment)
              }
            }
          )

        logger.debug(
          s"coverWithNonDefaultingFunds for $defaultingMember ${(cost - totalDefaultFunds) max 0}")

        (cost - totalDefaultFunds) max 0
      }
    }

  /**
    * Default coverage waterfall part of the non defaulting members.
    * @return cost after coverage
    */
  private def coverWithNonDefaultedCollateral(defaultingMember: ActorRef,
                                              cost: BigDecimal): Unit = {
    val f = coverWithNonDefaultingFunds(defaultingMember) andThen
        coverWithNonDefaultingInitialMargin(defaultingMember)

    self ! CollectUnfundedFunds(defaultingMember, f(cost))
  }

  /**
    * Covers the cost with the non defaulting members' unfunded funds.
    *
    * @return cost after coverage
    */
  private def collectUnfundedFunds(defaultingMember: ActorRef,
                                   cost: BigDecimal): Unit = {
    if (cost <= 0) {
      logger.debug(
        s"coverWithNonDefaultingUnfundedFunds for $defaultingMember 0")
      self ! WaterfallResult(Some(0))
    } else {
      implicit val timeout: Timeout = Timeout(60 seconds)
      val nonDefaultedMembers =
        allMembers diff defaultedMembers

      val fundPayments: Map[ActorRef, BigDecimal] =
        nonDefaultedMembers.flatMap(member => {
          for {
            proRata <- proRataDefaultFunds(member)
            fundLeft <- unfundedFundsLeft.get(member)

            _ = logger.debug(s"Fund left $fundLeft and proRata $proRata")

            unfundedCall = (cost * proRata) min fundLeft

            _ = unfundedFundsLeft
              .put(member, fundLeft - unfundedCall) // TODO needs its own function?
          } yield member -> unfundedCall
        })(breakOut)

      val waterfallId = generateUuid

      updateWaterfallWaiting(waterfallId,
                             fundPayments.size,
                             defaultingMember,
                             cost)

      fundPayments.foreach {
        case (member, fundPayment) =>
          val id = generateUuid
          logger.debug(s"EXPECTED $fundPayment")
          updateExpectedFundPayments(member, id, fundPayment)
          member ! UnfundedDefaultFundCall(id, waterfallId, fundPayment)
      }
    }
  }

  private def coverWithNonDefaultingUnfundedFunds(
      defaultingMember: ActorRef,
      cost: BigDecimal,
      collected: BigDecimal): Unit = {
    val costLeft = cost - collected

    if (costLeft <= 0) self ! WaterfallResult(Some(0))
    else self ! CoverWithSecondLevelEquity(defaultingMember, costLeft)
  }

  /**
    * Covers the cost with CCPs own equity. First step.
    * @return cost after coverage
    */
  private def coverWithFirstLevelEquity(defaultingMember: ActorRef,
                                        cost: BigDecimal): Unit = {
    if (cost <= 0) {
      self ! WaterfallResult(Some(0))
    } else {
      val costLeft = cost - equity
      val paid = cost min equity
      equity -= paid
      totalPaid += paid

      logger.debug(
        s"coverWithFirstLevelEquity for $defaultingMember to ${costLeft max 0}")

      self ! CoverWithNonDefaultingMembers(defaultingMember, costLeft max 0)
    }
  }

  /**
    * Covers the cost with the CCPs own equity. Last step.
    * @return cost after coverage
    */
  private def coverWithSecondLevelEquity(defaultingMember: ActorRef,
                                         cost: BigDecimal) = {
    if (cost <= 0) {
      self ! WaterfallResult(Some(0))
    } else {
      val costLeft = cost - equity
      val paid = cost min equity
      equity -= paid
      totalPaid += paid

      logger.debug(
        s"coverWithSecondLevelEquity for $defaultingMember to ${costLeft max 0}")

      self ! WaterfallResult(Some(0))
    }
  }

  /**
    * Updates the map m with the cost left while keeping the rule that it cannot contain a
    * negative value.
    * @param m map to update
    * @param member member to update
    * @param costLeft new value
    */
  private def updateAfterCost(m: mutable.Map[ActorRef, BigDecimal])(
      member: ActorRef,
      costLeft: BigDecimal
  ): Unit = {
    m.put(member, -costLeft max 0)
  }

  /**
    * Transforms the amount paid for adding to the total paid.
    */
  private val updatePaid: BigDecimal => BigDecimal = _ max 0

  /**
    * Updates its defaulted or not state depending on the cost left.
    * @param costLeft cost left
    */
  private def updateDefaulted(costLeft: BigDecimal): Unit = {
    if (costLeft > 0) {
      self ! Default
    }
  }

  /**
    * Generates a unique id.
    * @return the unique id
    */
  private def generateUuid = new RequestId(UUID.randomUUID().toString)

  /**
    * Updates the initial margin.
    * @param member member to update
    * @param initialMargin new initial margin
    */
  private def updateInitialMargin(
      member: ActorRef,
      initialMargin: BigDecimal
  ): Unit = {
    margins.put(member, initialMargin max 0)
  }

  /**
    * Updates the default fund.
    * @param member member to update
    * @param defaultFund new default fund
    */
  private def updateDefaultFund(
      member: ActorRef,
      defaultFund: BigDecimal
  ): Unit = {
    defaultFunds.put(member, defaultFund max 0)
  }

  /**
    * Updates the expected margin payments.
    * @param member member to update
    * @param payment expected payment
    */
  private def updateExpectedMarginPayments(
      member: ActorRef,
      id: RequestId,
      payment: BigDecimal
  ): Unit = {
    expectedMarginPayments.put((member, id), payment)
  }

  /**
    * Updates the expected fund payments.
    * @param member member to update
    * @param payment expected payment
    */
  private def updateExpectedFundPayments(
      member: ActorRef,
      id: RequestId,
      payment: BigDecimal
  ): Unit = {
    expectedDefaultFundPayments.put((member, id), payment)
  }

  private def updateWaterfallWaiting(id: RequestId,
                                     waitingFor: Int,
                                     defaultingMember: ActorRef,
                                     cost: BigDecimal): Unit = {
    expectedUnfundedFundsForWaterfall
      .put(id, (0, waitingFor, defaultingMember, cost))
  }
}

object Ccp {
  def props[A](
      name: String,
      memberPortfolios: Map[ActorRef, Portfolio[A]],
      ccpPortfolios: => Map[ActorRef, Portfolio[A]],
      equity: BigDecimal,
      rules: Rules,
      shouldDefault: Boolean = false
  ): Props = {
    Props(
      new Ccp(name,
              memberPortfolios,
              ccpPortfolios,
              equity,
              rules,
              shouldDefault))
  }

  /**
    * Rules of the CCP.
    *
    * @param marginIsRingFenced initial margin of non-defaulting members can be used
    * @param maximumFundCall maximum unfunded funds that can be called upon
    * @param memberRules rules for members
    * @param ccpRules rules for CCPs
    */
  case class Rules(
      marginIsRingFenced: Boolean,
      maximumFundCall: BigDecimal,
      minimumTransfer: BigDecimal,
      memberRules: MemberRules,
      ccpRules: CcpRules
  )

  case class MemberRules(marginCoverage: BigDecimal,
                         fundParticipation: BigDecimal)

  case class CcpRules(participatesInMargin: Boolean,
                      marginCoverage: BigDecimal,
                      fundParticipation: BigDecimal)

  case object TriggerMarginCalls
}
