package structure.ccp

import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import com.typesafe.scalalogging.Logger
import market.Portfolio
import market.Portfolio.{Transaction, buyAll, price, sellAll}
import structure.Scenario.Done
import structure.Scheduler.scheduledMessage
import structure.Timed._
import structure._
import structure.ccp.Ccp._
import structure.ccp.Waterfall.{Failed, _}
import util.Result
import util.Result.{Result, flatten}

import scala.util.Try
//import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps
import scalaz.Scalaz._
import scalaz._

/**
  * Models a central counterparty (CCP). Performs on demand margin calls and handles the subsequent
  * defaults through a cost waterfall.
  * @param name name of the CCP
  * @param memberPortfolios portfolios of each member it handles
  * @param ccpPortfolios portfolios of each ccp it handles
  * @param capital assets owned and their respective liquidity.
  * @param rules rules for configuring the CCP
  */
class Ccp(
    _name: String,
    waterfall: Waterfall,
//    memberPortfolios: Map[ActorRef, Portfolio],
//    ccpPortfolios: => Map[ActorRef, Portfolio],
    capital: Portfolio,
    rules: Rules,
    delays: OperationalDelays,
    _scheduler: ActorRef,
    _shouldDefault: Boolean
) extends Actor
    with MemberProcess {
  var memberPortfolios = Map.empty[ActorRef, Portfolio]
  var ccpPortfolios = Map.empty[ActorRef, Portfolio]
  var currentTime: Time = zero

  val name: String = _name
  val logger: Logger = Logger(name)
  val scheduler: ActorRef = _scheduler
  var _capital: Result[Portfolio] = Result.pure(capital)
  var movements: Map[Time, BigDecimal] = Map.empty
  val shouldDefault: Boolean = _shouldDefault

  def receive: Receive = {
    case Setup(_memberPortfolios, _ccpPortfolios) =>
//      logger.debug(s"${_ccpPortfolios.values.map(_.positions)}")
//      logger.debug(s"${_memberPortfolios.values.map(_.positions)}")
      currentTime = zero
      previousCallTime = zero
      _capital = Result.pure(capital)
      movements = Map.empty

      memberPortfolios = _memberPortfolios
      ccpPortfolios = _ccpPortfolios

      members = memberPortfolios.keys.toSet
      ccps = ccpPortfolios.keys.toSet

      defaultedMembers = Set.empty
      haircutsInProgress = 0
      paymentsDue = allMembers.map(_ -> BigDecimal(0)).toMap
      expectedPayments = Map.empty
      expectedUnfundedFunds = Map.empty

      margins = {
        if (rules.ccpRules.participatesInMargin) {
          for {
            (member, portfolio) <- allPortfolios
          } yield {
            member -> portfolio.margin(zero)(rules.marginCoverage)
          }
        } else {
          for {
            (member, portfolio) <- memberPortfolios
          } yield {
            member -> portfolio.margin(zero)(rules.marginCoverage)
          }
        }
      }

      initialMargins = margins

      defaultFunds = {
        def fundContribution(m: ActorRef) = {
          val funds = for {
            im <- flatten(initialMargins.get(m) \/> s"Could not get IM of $m")
          } yield im * rules.fundParticipation

          m -> funds
        }

        allMembers.map(fundContribution).toMap
      }

      initDefaultFunds = defaultFunds

      unfundedFundsLeft =
        allMembers.map(_ -> Result.pure(rules.maximumFundCall)).toMap

      sender ! Done

    case TriggerDefault(member, paymentLeft, t) =>
      triggerDefault(member, paymentLeft, t)

    case FinishedStage(currentStage, member, loss, t) =>
      nextStage(currentStage)(member, loss, t)

    case FailedResult(member, _) =>
      throw new IllegalStateException(s"Failed with $member")

    case TimedMessage(t, m) =>
      if (t < currentTime) {
        logger.warn("Received message from the past, time is +" + currentTime + ". " + m + " @" + t + " from " + sender)
      }
//      assert(
//        t >= currentTime,
//        "Received message from the past, time is +" + currentTime + ". " + m + " @" + t + " from " + sender)
      currentTime = currentTime max t

      val handle = handleMemberTimedMessages(t) orElse handleCcpProcesses(t)

      handle(m)
  }

  def handleCcpProcesses(t: Time): Receive = {
    case TriggerMarginCalls =>
      triggerMarginCalls(t)

    /* Responses */
    case MarginCallResponse(rId, responder, payment) =>
      handleMarginCallResponse(responder, rId, payment, t)

    case DefaultFundCallResponse(rId, responder, payment) =>
      handleDefaultFundCallResponse(responder, rId, payment, t)

    case UnfundedDefaultFundCallResponse(rId,
                                         waterfallId,
                                         responder,
                                         payment) =>
      handleUnfundedDefaultFundCallResponse(responder,
                                            rId,
                                            waterfallId,
                                            payment,
                                            t)

    /* Waterfall */
    case CoverWithDefaulted(member, loss) =>
      coverWithDefaultedCollateral(member, loss, t)

    case CoverWithSurvivors(member, loss) =>
      coverWithNonDefaultedCollateral(member, loss, t)

    case CollectUnfunded(member, loss) =>
      collectUnfunded(member, loss, t)

    case CoverWithUnfunded(member, loss, collected) =>
      coverWithUnfunded(member, loss, collected, t)

    case CoverWithVMGH(member, loss) =>
      coverWithVMGH(member, loss, t)

    case CoverWithFirstLevelEquity(member, loss) =>
      coverWithFirstLevelEquity(member, loss, t)

    case CoverWithSecondLevelEquity(member, loss) =>
      coverWithSecondLevelEquity(member, loss, t)

    case WaterfallResult(member, loss) =>
      handleWaterfallResult(member, loss, t)
  }

//  private val logger = Logger(name)

//  private var currentTime = zero
  private var previousCallTime = zero

//  private var _capital = Result.pure(capital)
//  private var movements = Map.empty[Time, BigDecimal]

  private var members: Set[ActorRef] = Set.empty
  private var ccps: Set[ActorRef] = Set.empty
  private def allMembers: Set[ActorRef] = members ++ ccps

  private def allPortfolios: Map[ActorRef, Portfolio] =
    memberPortfolios ++ ccpPortfolios

  private var defaultedMembers: Set[ActorRef] = Set.empty

  private var paymentsDue: Map[ActorRef, BigDecimal] = Map.empty
  private val vmgh = waterfall.stages.contains(VMGH)
  private var haircutsInProgress = 0
  private def shouldHoldPayments = vmgh && haircutsInProgress > 0

  /**
    * The prices of the portfolios at time t.
    *
    * @param t point in time of the prices.
    * @return the portfolio prices of each member.
    */
  private def portfolioPrice(member: ActorRef, t: Time): Result[BigDecimal] = {
    flatten(for {
      p <- allPortfolios.get(member) \/> s"Could not get portfolio of $member."
    } yield price(p)(t))
  }

  /**
    * Posted margins of members.
    */
  private var margins: Map[ActorRef, Result[BigDecimal]] = Map.empty

  /**
    * Snapshot of the initial margins when setting up the CCP.
    */
  private var initialMargins: Map[ActorRef, Result[BigDecimal]] = Map.empty

  /**
    * Posted default funds of members.
    */
  private var defaultFunds: Map[ActorRef, Result[BigDecimal]] = Map.empty

  /**
    * Snapshot of the default funds when setting up the CCP.
    */
  private var initDefaultFunds: Map[ActorRef, Result[BigDecimal]] = Map.empty

  /**
    * How much unfunded funds can still be called upon.
    */
  private var unfundedFundsLeft: Map[ActorRef, Result[BigDecimal]] = Map.empty

  /**
    * Computes the percentage of the initial default funds of the member ex defaulted members.
    *
    * @param member member for which to compute the pro-rata
    * @return percentage of the default funds of the member ex defaulted members.
    */
  private def proRataDefaultFunds(member: ActorRef): Result[BigDecimal] = {
    val currentDefaulted = defaultedMembers

    val survivingMembersFunds =
      initDefaultFunds.withFilter(e => !currentDefaulted.contains(e._1))

    val totalFundsF =
      survivingMembersFunds.map(_._2).toList.map(_.map(_ max 0)).suml

    for {
      part <- flatten(
        initDefaultFunds.get(member) \/> s"Could not get IDF from $member.")
      totalFunds <- totalFundsF
    } yield Try(part / totalFunds).getOrElse(BigDecimal(0))
  }

  /**
    * Computes the percentage of the initial margins of the member ex defaulted members.
    *
    * @param member member for which to compute the pro-rata
    * @return percentage of the initial margins of the member ex defaulted members.
    */
  private def proRataInitialMargins(
      member: ActorRef
  ): Result[BigDecimal] = {
    val currentDefaulted = defaultedMembers

    val survivingMembersMargins =
      initialMargins.withFilter(e => !currentDefaulted.contains(e._1))

    val totalMarginsF =
      survivingMembersMargins.map(_._2).toList.map(_.map(_ max 0)).suml

    for {
      margin <- flatten(
        initialMargins.get(member) \/> s"Could not get IM from $member.")
      totalMargins <- totalMarginsF
    } yield Try((margin max 0) / totalMargins).getOrElse(BigDecimal(0))
  }

  /**
    * Expected payments after calls.
    */
  private var expectedPayments: Map[(ActorRef, RequestId), BigDecimal] =
    Map.empty

  private case class UnfundedBuffer(collected: BigDecimal,
                                    waitingFor: Int,
                                    defaultedMember: ActorRef,
                                    loss: BigDecimal)

  private var expectedUnfundedFunds: Map[WaterfallId, UnfundedBuffer] =
    Map.empty

  /**
    * Triggers margin calls at time t.
    *
    * @param t time of margin call.
    */
  private def triggerMarginCalls(t: Time): Unit = {
    //    implicit val timeout: Timeout = Timeout(60 seconds)

    val survivors = allMembers -- defaultedMembers
    survivors foreach (memberMarginCall(_, t))

    previousCallTime = t
  }

  /**
    * Margin call to member m at time t.
    *
    * @param member member to call.
    * @param t      time of call.
    */
  private def memberMarginCall(member: ActorRef, t: Time): Unit = {
    val currentMargins = margins
    val currentPaymentsDue = paymentsDue

    val currentMarginF = for {
      previousMargin <- flatten(currentMargins
        .get(member) \/> s"Could not get previous previous margin from $member.")

      oldPrice <- portfolioPrice(member, previousCallTime)

      currentPrice <- portfolioPrice(member, t)
    } yield previousMargin + (currentPrice - oldPrice)

    margins += member -> currentMarginF

    val marginCallF = (for {
      initialMargin <- flatten(
        initialMargins.get(member) \/> s"Could not get IM from $member.")
      currentMargin <- currentMarginF

      marginCall = initialMargin - currentMargin
    } yield marginCall)
      .ensure("Call smaller than MTA.")(_.abs >= rules.minimumTransfer)

    for {
      marginCall <- marginCallF.ensure(
        s"No need to hold payments for $member @$t")(
        _ < 0 && shouldHoldPayments)

      currentDue <- currentPaymentsDue.get(member) \/> s"Could not get dues of $member"

      _ = paymentsDue += member -> (currentDue - marginCall)
    } yield ()

//    paymentsDue += member -> newDue

    val outboundMarginCallF =
      marginCallF.ensure("Should not send out request.")(
        _ > 0 || !shouldHoldPayments)

    val request = for {
      mc <- outboundMarginCallF
      m = if (mc > 0) {
        val id = generateRequestId
        expectedPayments += (member, id) -> mc
        scheduledMessage(t, member, MarginCall(id, mc, rules.maxCallPeriod))
      } else {
        scheduledMessage(t, member, Transfer(-mc))
      }
    } yield m

    request.fold(e => e, scheduler ! _)
  }

  /**
    * Handles what happens after a margin call response.
    * @param member member that responded.
    * @param id id of the margin call request.
    * @param payment amount paid.
    * @param t time received.
    */
  private def handleMarginCallResponse(
      member: ActorRef,
      id: RequestId,
      payment: BigDecimal,
      t: Time
  ): Unit = {
    require(payment >= 0,
            s"Received negative payment from $member. $payment @$t.")

    val currentMargins = margins

    val newMargin = for {
      currentMargin <- flatten(
        currentMargins.get(member) \/> s"Could not get margin of $member.")
    } yield currentMargin + payment

    margins += member -> newMargin

    checkAndTrigger(member, id, payment, t)
  }

  /**
    * Handles what happens after a default fund call response.
    * @param member member that responded.
    * @param id id of the fund call request.
    * @param payment amount paid.
    * @param t time received.
    */
  private def handleDefaultFundCallResponse(
      member: ActorRef,
      id: RequestId,
      payment: BigDecimal,
      t: Time
  ): Unit = {
    val currentDefaultFunds = defaultFunds

    val updateFund = for {
      currentFund <- flatten(
        currentDefaultFunds.get(member) \/> s"Could not get fund of $member.")
    } yield currentFund + payment

    defaultFunds += member -> updateFund

    checkAndTrigger(member, id, payment, t)
  }

  private def checkAndTrigger(member: ActorRef,
                              id: RequestId,
                              payment: BigDecimal,
                              t: Time) = {
    val currentExpectedPayments = expectedPayments

    val paymentLeftMessage = for {
      expectedPayment <- (currentExpectedPayments
        .get((member, id)) \/> s"Could not get expected payment of $member. $id")
        .ensure("OK")(payment < _)
    } yield TriggerDefault(member, expectedPayment - payment, t)

    paymentLeftMessage.fold(e => e, self ! _)

    expectedPayments -= ((member, id))
  }

  /**
    * Handles what happens after an unfunded default fund call response.
    * @param member member that responded.
    * @param id id of the unfunded fund call request.
    * @param waterfallId id of the waterfall in which the unfunded fund call request was made.
    * @param payment amount paid.
    * @param t time received.
    */
  private def handleUnfundedDefaultFundCallResponse(
      member: ActorRef,
      id: RequestId,
      waterfallId: WaterfallId,
      payment: BigDecimal,
      t: Time
  ): Unit = {
    checkAndTrigger(member, id, payment, t)
    val currentExpectedUnfundedFunds = expectedUnfundedFunds

    for {
      b <- currentExpectedUnfundedFunds.get(waterfallId) \/> s"Could not get expected unfunded for member $member @$t."

      _ = if (b.waitingFor == 1) {
        // Finished waiting
        scheduleMessage(
          t,
          self,
          CoverWithUnfunded(b.defaultedMember, b.loss, b.collected + payment))
        expectedUnfundedFunds -= waterfallId
      } else {

        // Update and wait
        expectedUnfundedFunds += waterfallId -> b.copy(
          collected = b.collected + payment,
          waitingFor = b.waitingFor - 1)
      }
    } yield ()
  }

  /**
    * Starts the waterfall process for the member.
    * @param member member the defaulted.
    * @param loss amount that was not paid.
    * @param t time of the default.
    */
  private def triggerDefault(
      member: ActorRef,
      loss: BigDecimal,
      t: Time
  ): Unit = {
    if (loss > BigDecimal(0.0001)) {
      if (!defaultedMembers.contains(member)) {
        defaultedMembers += member

        val cover = for {
          portfolio <- allPortfolios.get(member) \/> s"Could not get portfolio of $member."
          (replacementCost, timeToReplace) <- portfolio.replacementCost(t)
          _ = logger.debug(s"$member -- PFOLIO ${replacementCost}")
        } yield
          (FinishedStage(Start, member, loss, t),
            FinishedStage(Start,
              member,
              replacementCost max 0,
              t + timeToReplace))

        // Margin payments are not paid during waterfalls if VMGH is used.
        haircutsInProgress += 2

        cover.map(_._1).fold(e => throw new IllegalStateException(e), self ! _)
        cover.map(_._2).fold(e => throw new IllegalStateException(e), self ! _)
      } else {
        // Happens when the CCP is waiting for two calls, and the member defaults on both.
        haircutsInProgress += 1
        self ! FinishedStage(Start, member, loss, t)
      }
    }
  }

  /**
    * Chooses what needs to be done after all the waterfall steps have been exhausted.
    * @param loss losses left.
    * @param t time
    */
  private def handleWaterfallResult(member: ActorRef,
                                    loss: BigDecimal,
                                    t: Time): Unit = {
    if (waterfall.stages.keys.exists(_ == VMGH))
      tryReleasePayments(t)

    logger.debug(s"END $loss for member $member")
  }

  /**
    * Cover the losses with the collateral from the defaulted member.
    * @param defaultedMember member that defaulted.
    * @param loss losses that need to be covered.
    * @param t time the covering started.
    */
  private def coverWithDefaultedCollateral(defaultedMember: ActorRef,
                                           loss: BigDecimal,
                                           t: Time): Unit = {

    /**
      * Covers the losses with the defaulting member' posted initial margin.
      * @param defaultedMember the member that has to cover the losses
      * @return covered losses
      */
    def coverWithInitialMargin(defaultedMember: ActorRef) =
      Kleisli[Result, BigDecimal, BigDecimal](loss => {
        if (loss <= 0) {
          Result.pure(0)
        } else {
          val currentMargins = margins

          val currentMarginF =
            flatten(currentMargins
              .get(defaultedMember) \/> s"Could not get margin of $defaultedMember.")

          // Left after using margin
          val lossAfterMarginUse = for {
            currentMargin <- currentMarginF
          } yield loss - (currentMargin max 0)

          val newMargin = for {
            currentMargin <- currentMarginF
          } yield (currentMargin - loss) max 0

          margins += defaultedMember -> newMargin

          lossAfterMarginUse.map(_ max 0)
        }
      })

    /**
      * Covers the losses with the defaulting member' posted default fund contribution.
      * @param defaultedMember the member that has to cover the losses
      * @return covered losses
      */
    def coverWithFund(defaultedMember: ActorRef) =
      Kleisli[Result, BigDecimal, BigDecimal](loss => {
        if (loss <= 0) {
          Result.pure(0)
        } else {
          val currentDefaultFunds = defaultFunds

          val currentFundF = flatten(currentDefaultFunds
            .get(defaultedMember) \/> s"Could not get fund of $defaultedMember.")

          // Left after funds use
          val lossAfterFundUse = for {
            currentFund <- currentFundF
          } yield loss - (currentFund max 0)

          val newFund = for {
            currentFund <- currentFundF
          } yield currentFund - (loss min currentFund)

          defaultFunds += defaultedMember -> newFund

          lossAfterFundUse.map(_ max 0)
        }
      })

    val currentMargins = margins
    val currentDefaultFunds = defaultFunds

    val coverWithCollateral = coverWithInitialMargin(defaultedMember) andThen coverWithFund(
        defaultedMember)

    val delayedT = t + (delays.coverWithMargin max delays.coverWithFund)

    // Ask from cover with collateral
    val margin = flatten(
      currentMargins
        .get(defaultedMember) \/> s"Could not get margin of $defaultedMember.")

    val fund = flatten(
      currentDefaultFunds
        .get(defaultedMember) \/> s"Could not get fund of $defaultedMember.")

    // Not waiting for payments
    if (!expectedPayments.keys.exists(_._1 == defaultedMember)) {
      val transfer = (margin |@| fund) { (m, f) =>
        // Margin account might be negative if heavily defaulted.
        scheduledMessage(t, defaultedMember, Transfer((m + f) max 0))
      }

      transfer.fold(e => throw new IllegalStateException(e), scheduler ! _)
    }

    val nextStageMessage = for {
      l <- coverWithCollateral(loss)
    } yield FinishedStage(Defaulted, defaultedMember, l, delayedT)

    val toFailedStage = FinishedStage(Failed, defaultedMember, 0, delayedT)

    self ! (nextStageMessage | toFailedStage)
  }

  /**
    * Covers the loss with the collateral posted by the surviving members.
    * @param defaultedMember member that defaulted
    * @param loss losses to cover
    * @param t time
    */
  private def coverWithNonDefaultedCollateral(defaultedMember: ActorRef,
                                              loss: BigDecimal,
                                              t: Time): Unit = {

    /**
      * Covers the losses with the surviving members margin.
      * @param defaultedMember member that defaulted.
      * @return losses after covering with margins.
      */
    def coverWithSurvivingMargin(defaultedMember: ActorRef) =
      Kleisli[Result, BigDecimal, BigDecimal](loss => {
        if (loss <= 0) {
          Result.pure(0)
        } else {
          val survivingMembers = allMembers -- defaultedMembers
          val currentMargins = margins

          val survivingMemberMargins =
            currentMargins.withFilter(entry =>
              survivingMembers.contains(entry._1))

          val totalMarginsF =
            survivingMemberMargins
              .map(_._2)
              .toList
              .map(_.map(_ max 0))
              .suml

          survivingMembers.foreach(
            member => {
              val currentMarginF =
                flatten(
                  currentMargins
                    .get(member) \/> s"Could not get margin of $member.")

              val paymentF = for {
                proRata <- proRataInitialMargins(member)
                totalMargins <- totalMarginsF
                payment = (loss min totalMargins) * proRata
              } yield payment

              val newMarginF = for {
                currentMargin <- currentMarginF
                payment <- paymentF
              } yield currentMargin - payment

              margins += member -> newMarginF

              val marginCall = for {
                payment <- paymentF
                id = generateRequestId
                _ = expectedPayments += (member, id) -> payment
              } yield
                scheduledMessage(
                  t + delays.coverWithSurvivorsMargins,
                  member,
                  LossMarginCall(id, payment, rules.maxCallPeriod))

              marginCall.fold(e => throw new IllegalStateException(e),
                              scheduler ! _)
            }
          )

          totalMarginsF.map(totalMargins => (loss - totalMargins) max 0)
        }
      })

    /**
      * Covers the losses with the surviving members funds.
      * @param defaultedMember member that defaulted.
      * @return losses after covering with funds.
      */
    def coverWithNonDefaultingFunds(defaultedMember: ActorRef) =
      Kleisli[Result, BigDecimal, BigDecimal](loss => {
        if (loss <= 0) {
          Result.pure(0)
        } else {
          val survivingMembers = allMembers -- defaultedMembers
          val currentDefaultFunds = defaultFunds

          val survivingMemberFunds =
            currentDefaultFunds.withFilter(entry =>
              survivingMembers.contains(entry._1))

          val totalFundsF =
            survivingMemberFunds
              .map(_._2)
              .toList
              .map(_.map(_ max 0))
              .suml

          survivingMembers.foreach {
            member =>
              val currentFundF =
                flatten(
                  currentDefaultFunds
                    .get(member) \/> s"Could not get fund of $member.")

              val paymentF = for {
                proRata <- proRataDefaultFunds(member)
                totalFunds <- totalFundsF
                payment = (loss min totalFunds) * proRata
              } yield payment

              val newFundF = for {
                currentFund <- currentFundF
                payment <- paymentF
              } yield currentFund - payment

              defaultFunds += member -> newFundF

              val fundCall = for {
                payment <- paymentF
                id = generateRequestId
                _ = expectedPayments += (member, id) -> payment
              } yield
                scheduledMessage(
                  t + delays.coverWithSurvivorsFunds,
                  member,
                  DefaultFundCall(id, payment, rules.maxRecapPeriod))

              fundCall.fold(e => throw new IllegalStateException(e),
                            scheduler ! _)
          }

          totalFundsF.map(totalFunds => (loss - totalFunds) max 0)
        }
      })

    val coverWithSurvivors = coverWithNonDefaultingFunds(defaultedMember) andThen
        coverWithSurvivingMargin(defaultedMember)

    val delay =
      if (rules.marginIsRingFenced) delays.coverWithSurvivorsFunds
      else delays.coverWithMargin max delays.coverWithFund

    val delayedT = t + delay

    val nextStageMessage = for {
      l <- coverWithSurvivors(loss)
    } yield FinishedStage(Survivors, defaultedMember, l, delayedT)

    val toFailedStage = FinishedStage(Failed, defaultedMember, 0, delayedT)

    self ! (nextStageMessage | toFailedStage)
  }

  /**
    * Collects the unfunded funds in preparation for using it to cover the losses.
    * @param defaultedMember member that defaulted.
    * @param loss losses to cover.
    * @param t time
    */
  private def collectUnfunded(defaultedMember: ActorRef,
                              loss: BigDecimal,
                              t: Time): Unit = {
    val survivingMembers = allMembers -- defaultedMembers
    val waterfallId = generateWaterfallId
    val currentUnfundedLeft = unfundedFundsLeft

    survivingMembers.foreach { member =>
      val unfundedLeftF =
        flatten(
          currentUnfundedLeft
            .get(member) \/> s"Could not get unfunded for $member.")

      val paymentF = for {
        unfundedLeft <- unfundedLeftF
        proRata <- proRataDefaultFunds(member)

        // Don't call more than allowed
        payment = (loss * proRata) min unfundedLeft
      } yield payment

      val newFundLeftF = (unfundedLeftF |@| paymentF) { _ - _ }
      unfundedFundsLeft += member -> newFundLeftF

      val id = generateRequestId

      val unfundedFundCallMessage = for {
        payment <- paymentF
        _ = expectedPayments += (member, id) -> payment
      } yield
        scheduledMessage(t + delays.computeSurvivorsUnfunded,
                         member,
                         UnfundedDefaultFundCall(id,
                                                 waterfallId,
                                                 payment,
                                                 rules.maxCallPeriod))

      unfundedFundCallMessage.fold(e => throw new IllegalStateException(e),
                                   scheduler ! _)
    }

    expectedUnfundedFunds += waterfallId -> UnfundedBuffer(
      0,
      survivingMembers.size,
      defaultedMember,
      loss)
  }

  /**
    * Covers the losses with the collected unfunded funds.
    * @param defaultedMember member that defaulted.
    * @param loss losses to cover.
    * @param collected amount collected.
    * @param t time
    */
  private def coverWithUnfunded(defaultedMember: ActorRef,
                                loss: BigDecimal,
                                collected: BigDecimal,
                                t: Time): Unit = {
    val lossLeft = loss - collected

    val delayedT = t + delays.coverWithSurvivorsUnfunded

    self ! FinishedStage(Unfunded, defaultedMember, lossLeft max 0, delayedT)
  }

  private def coverWithVMGH(defaultedMember: ActorRef,
                            loss: BigDecimal,
                            t: Time): Unit = {
    val currentPaymentsDue = paymentsDue

    val totalDue = currentPaymentsDue.values.sum

    if (totalDue > 0) {
      val available = loss min totalDue

      val haircuts = currentPaymentsDue.map {
        case (m, due) =>
          (m, available * (due / totalDue))

        //        (m, (due |@| totalDue.ensure("Zero total due.")(_ > 0) |@| available) {
        //          (d, tot, av) =>
        //            av * (d / tot)
        //        })
      }

      val haircut = for {
        (m, due) <- currentPaymentsDue
        haircut <- haircuts.get(m)
      } yield m -> -haircut

      haircut.foreach {
        case (m, haircut) => scheduleMessage(t, m, VMGHLoss(-haircut))
      }

      paymentsDue |+|= haircut

      self ! FinishedStage(VMGH,
                           defaultedMember,
                           loss - available,
                           t + delays.coverWithVMGH)
    }

    self ! FinishedStage(VMGH, defaultedMember, loss, t)
  }

  /**
    * Releases the variation margins due if no haircut is in process.
    * @param t time to release.
    */
  private def tryReleasePayments(t: Time): Unit = {
    haircutsInProgress -= 1

    val currentPaymentsDue = paymentsDue

    // Can release the payments due
    if (haircutsInProgress == 0) {
      val paymentDueMessages = currentPaymentsDue.map {
        case (m, due) =>
//          require(due >= 0, s"Due was $due")
          scheduledMessage(t, m, Transfer(due))
      }

      paymentDueMessages.foreach(scheduler ! _)
    }
  }

  /**
    * Covers the losses with a certain percentage of the CCPs equity.
    * @param defaultedMember member that defaulted.
    * @param loss losses to cover.
    * @param t time
    */
  private def coverWithFirstLevelEquity(defaultedMember: ActorRef,
                                        loss: BigDecimal,
                                        t: Time): Unit = {
    val currentCapital = _capital

    val totalEquityF = currentCapital.flatMap(price(_)(t))

    val equityForFirstLevelF = totalEquityF.map(_ * rules.skinInTheGame)

    val toSellF = equityForFirstLevelF.map(_ min loss)

    val transaction = for {
      c <- currentCapital
      toSell <- toSellF
      t <- sellAll(c)(toSell, t)
    } yield t

    val newCapitalF = transaction.map(_.portfolio)
    val timeToSellF = transaction.map(_.timeToPerform)
    val amountF = transaction.map(_.transactionAmount)

    // Fails if selling failed
    _capital = newCapitalF

    val nextStageMessage = for {
      timeToSell <- timeToSellF
      amount <- amountF
    } yield
      FinishedStage(FirstLevelEquity,
                    defaultedMember,
                    (loss - amount) max 0,
                    t + timeToSell)

    nextStageMessage.fold(e => throw new IllegalStateException(e), self ! _)
  }

  /**
    * Covers the losses with a certain percentage of the CCPs equity.
    * @param defaultedMember member that defaulted.
    * @param loss losses to cover.
    * @param t time
    */
  private def coverWithSecondLevelEquity(defaultedMember: ActorRef,
                                         loss: BigDecimal,
                                         t: Time): Unit = {
    val currentCapital = _capital

    val totalEquityF = currentCapital.flatMap(price(_)(t))

    val toSellF = totalEquityF.map(_ min loss)

    val transaction = for {
      c <- currentCapital
      toSell <- toSellF
      t <- sellAll(c)(toSell, t)
    } yield t

    val newCapitalF = transaction.map(_.portfolio)
    val timeToSellF = transaction.map(_.timeToPerform)
    val amountF = transaction.map(_.transactionAmount)

    // Fails if selling failed
    _capital = newCapitalF

    val nextStageMessage = for {
      timeToSell <- timeToSellF
      amount <- amountF
    } yield
      FinishedStage(SecondLevelEquity,
                    defaultedMember,
                    (loss - amount) max 0,
                    t + timeToSell)

    nextStageMessage.fold(e => throw new IllegalStateException(e), self ! _)
  }

  private def generateUUID = UUID.randomUUID().toString

  /**
    * Generates a unique id.
    * @return the unique id
    */
  private def generateRequestId = new RequestId(UUID.randomUUID().toString)
  private def generateWaterfallId = new WaterfallId(UUID.randomUUID().toString)

  /**
    * Continues the waterfall with the next stage.
    * It shortcircuits the waterfall is there is no loss left.
    * @param currentStage the stage that is being currently run.
    * @param member the member that has defaulted.
    * @param loss the loss left to be covered.
    * @param t time at which to continue.
    */
  private def nextStage(currentStage: WaterfallStage)(member: ActorRef,
                                                      loss: BigDecimal,
                                                      t: Time): Unit = {
    assert(loss >= 0,
           s"Too much has been used during stage $currentStage - $loss")

//    logger.debug(s"Finished $currentStage with $loss @$t ($member)")

    def toInternalStage(s: WaterfallStage)(member: ActorRef,
                                           loss: BigDecimal): InternalStage =
      s match {
        case Start => throw new IllegalArgumentException()
        case Defaulted => CoverWithDefaulted(member, loss)
        case Survivors => CoverWithSurvivors(member, loss)
        case Unfunded => CollectUnfunded(member, loss)
        case VMGH => CoverWithVMGH(member, loss)
        case FirstLevelEquity => CoverWithFirstLevelEquity(member, loss)
        case SecondLevelEquity => CoverWithSecondLevelEquity(member, loss)
        case End => WaterfallResult(member, loss)
        case Failed => FailedResult(member, loss)
      }

    if (currentStage == Failed)
      self ! toInternalStage(currentStage)(member, loss)

    if (loss > 0) {
      // No need to keep payments if VMGH is done.
      if (currentStage == VMGH) tryReleasePayments(t)

      val switchStateMessageO = for {
        s <- waterfall.next(currentStage)
        message = toInternalStage(s)(member, loss)
      } yield scheduledMessage(t, self, message)

      scheduler ! switchStateMessageO.getOrElse(
        throw new IllegalStateException("Next stage is not defined."))
    } else {
      // Nothing left to do... shortcircuit the waterfall.
//      if (waterfall.stages.keys.exists(_ == VMGH)) {
//        tryReleasePayments(t)
//      }

      scheduleMessage(t, self, WaterfallResult(member, loss))
    }
  }
}

object Ccp {
  def props[A](name: String,
               waterfall: Waterfall,
               capital: Portfolio,
               rules: Rules,
               operationalDelays: OperationalDelays,
               scheduler: ActorRef,
               shouldDefault: Boolean = false): Props = {
    Props(
      new Ccp(name,
              waterfall,
              capital,
              rules,
              operationalDelays,
              scheduler,
              shouldDefault))
  }

  case class Setup(memberPortfolios: Map[ActorRef, Portfolio],
                   ccpPortfolios: Map[ActorRef, Portfolio])
  case object TriggerMarginCalls

  /**
    * Rules of the CCP.
    *
    * @param marginIsRingFenced initial margin of non-defaulting members can be used
    * @param maximumFundCall maximum unfunded funds that can be called upon
    * @param ccpRules rules for CCPs
    */
  case class Rules(maxCallPeriod: Time,
                   maxRecapPeriod: Time,
                   minimumTransfer: BigDecimal,
                   marginIsRingFenced: Boolean,
                   maximumFundCall: BigDecimal,
                   skinInTheGame: BigDecimal,
                   marginCoverage: BigDecimal,
                   fundParticipation: BigDecimal,
                   ccpRules: CcpRules) {
    require(maximumFundCall >= 0)
    require(minimumTransfer >= 0)
  }

  case class CcpRules(participatesInMargin: Boolean)

  case class OperationalDelays(
      callHandling: Time,
      coverWithMargin: Time,
      coverWithFund: Time,
      coverWithSurvivorsMargins: Time,
      coverWithSurvivorsFunds: Time,
      computeSurvivorsUnfunded: Time,
      coverWithSurvivorsUnfunded: Time,
      coverWithVMGH: Time,
      coverWithFirstLevelEquity: Time,
      coverWithSecondLevelEquity: Time
  )

  private trait InternalStage
  private case class CoverWithDefaulted(member: ActorRef, loss: BigDecimal)
      extends InternalStage
  private case class CoverWithSurvivors(member: ActorRef, loss: BigDecimal)
      extends InternalStage
  private case class CollectUnfunded(member: ActorRef, loss: BigDecimal)
      extends InternalStage
  private case class CoverWithUnfunded(member: ActorRef,
                                       loss: BigDecimal,
                                       collected: BigDecimal)
      extends InternalStage
  private case class CoverWithVMGH(member: ActorRef, loss: BigDecimal)
      extends InternalStage
  private case class CoverWithFirstLevelEquity(member: ActorRef,
                                               loss: BigDecimal)
      extends InternalStage
  private case class CoverWithSecondLevelEquity(member: ActorRef,
                                                loss: BigDecimal)
      extends InternalStage
  private case class WaterfallResult(member: ActorRef, loss: BigDecimal)
      extends InternalStage
  private case class FailedResult(member: ActorRef, loss: BigDecimal)
      extends InternalStage

  private case class AddExpectedPayment(member: ActorRef,
                                        id: RequestId,
                                        payment: BigDecimal)
  private case class RemoveExpectedPayment(member: ActorRef, id: RequestId)
  private case class UpdateFund(member: ActorRef, fund: BigDecimal)
  private case class UpdateUnfunded(member: ActorRef, unfund: BigDecimal)
  private case class AddDefaulted(member: ActorRef)
  private case class TriggerDefault(member: ActorRef,
                                    paymentLeft: BigDecimal,
                                    t: Time)
  private case class FinishedStage(currentStage: WaterfallStage,
                                   member: ActorRef,
                                   loss: BigDecimal,
                                   t: Time)
}
