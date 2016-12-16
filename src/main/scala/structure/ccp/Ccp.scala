package structure.ccp

import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import cats.data.{Kleisli, OptionT}
import cats.implicits._
import com.typesafe.scalalogging.Logger
import market.Portfolio
import structure.Scheduler.{TriggerMarginCalls, scheduledMessage}
import structure.Timed._
import structure._
import structure.ccp.Ccp._
import structure.ccp.Waterfall._
import util.PaymentSystem

import scala.annotation.tailrec
import scala.collection.breakOut
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.Try

/**
  * Models a central counterparty (CCP). Performs on demand margin calls and handles the subsequent
  * defaults through a cost waterfall.
  * @param name name of the CCP
  * @param memberPortfolios portfolios of each member it handles
  * @param ccpPortfolios portfolios of each ccp it handles
  * @param assets assets owned and their respective liquidity.
  * @param rules rules for configuring the CCP
  * @tparam A type of the instruments the CCP handles.
  */
class Ccp[A](
    name: String,
    waterfall: Waterfall,
    memberPortfolios: Map[ActorRef, Portfolio],
    ccpPortfolios: => Map[ActorRef, Portfolio],
    assets: Map[Time, BigDecimal],
    rules: Rules,
    delays: OperationalDelays,
    scheduler: ActorRef
) extends Actor
    with PaymentSystem {
  override def receive: Receive = LoggingReceive {
    case Run(timeHorizon: Time) =>
      scheduleMarginCalls(zero)

      /**
        * Recursively schedules margin calls from time t up to the time horizon.
        * @param t start scheduling at from this time.
        */
      @tailrec
      def scheduleMarginCalls(t: Time): Unit = {
        if (t <= timeHorizon) {
          scheduleMessage(t, self, TriggerMarginCalls)

          // Schedule next
          scheduleMarginCalls(t + rules.callEvery)
        }
      }

    case Paid =>
      sender ! totalPaid

    case AddExpectedPayment(m, id, p) => expectedPayments += (m, id) -> p
    case RemoveExpectedPayment(m, id) => expectedPayments -= ((m, id))

    case UpdateMargin(member, margin) => margins = margins.map(ms => ms.+((member, margin)))
    case UpdateFund(member, fund) => defaultFunds = defaultFunds.map(fs => fs.+((member, fund)))
    case UpdateUnfunded(member, unfund) => unfundedFundsLeft += member -> unfund
    case AddDefaulted(member) => defaultedMembers += member
    case AddPaymentDue(member, payment) => paymentsDue += member -> payment

    case TimedMessage(t, m) =>
      assert(t >= currentTime,
             "Received message from the past, time is +" + currentTime + ". " + m + " @ " + t)
      currentTime = currentTime max t

      m match {
        case Paid =>
        case TriggerMarginCalls => triggerMarginCalls(t)

        /* Calls */
        case MarginCall(id, payment, maxDelay) =>
          if (payment < 0) updateAssets(-payment)
          else {
            val u = handlePayment(_assets, payment, maxDelay - delays.callHandling)
            update(u.assets, u.timedPayment.payment)
            scheduleMessage(t + u.timedPayment.delay + delays.callHandling,
                            sender,
                            MarginCallResponse(id, self, u.timedPayment.payment))

          }

        case DefaultFundCall(id, payment, maxDelay) =>
          val u = handlePayment(_assets, payment, maxDelay - delays.callHandling)
          update(u.assets, u.timedPayment.payment)
          scheduleMessage(t + u.timedPayment.delay + delays.callHandling,
                          sender,
                          DefaultFundCallResponse(id, self, u.timedPayment.payment))

        case UnfundedDefaultFundCall(id, waterfallId, payment, maxDelay) =>
          val u = handlePayment(_assets, payment, maxDelay - delays.callHandling)
          update(u.assets, u.timedPayment.payment)
          scheduleMessage(
            t + u.timedPayment.delay + delays.callHandling,
            sender,
            UnfundedDefaultFundCallResponse(waterfallId, id, self, u.timedPayment.payment))

        /* Responses */
        case MarginCallResponse(id, responder, payment) =>
          handleMarginCallResponse(responder, id, payment, t)

        case DefaultFundCallResponse(id, responder, payment) =>
          handleDefaultFundCallResponse(responder, id, payment, t)

        case UnfundedDefaultFundCallResponse(waterfallId, id, responder, payment) =>
          handleUnfundedDefaultFundCallResponse(responder, id, waterfallId, payment, t)

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

        case WaterfallResult(member, loss) => handleWaterfallResult(member, loss, t)
      }
  }

  private val logger = Logger(name)

  private var currentWaterfallStage: WaterfallStage = Start

  private var currentTime = zero
  private var previousCallTime = zero

  private var _assets = assets
  private var totalPaid: BigDecimal = 0

  private val members: Set[ActorRef] = memberPortfolios.keys.toSet
  private val ccps: Set[ActorRef] = ccpPortfolios.keys.toSet
  private val allMembers: Set[ActorRef] = members ++ ccps

  private val allPortfolios: Map[ActorRef, Portfolio] = memberPortfolios ++ ccpPortfolios

  private var defaultedMembers: Set[ActorRef] = Set.empty

  private var paymentsDue: Map[ActorRef, BigDecimal] = Map.empty
  private val vmgh = waterfall.stages.contains(VMGH)
  private var haircutsInProgress = 0
  private val holdPayments = vmgh && haircutsInProgress > 0

  /**
    * The prices of the portfolios at time t.
    * @param t point in time of the prices.
    * @return the portfolio prices of each member.
    */
  private def portfolioPrices(t: Time): Future[Map[ActorRef, BigDecimal]] = {
    val prices = for {
      (member, portfolio) <- allPortfolios
    } yield (member, portfolio.price(t))

    val pricesO = Future.traverse(prices) { case (m, fo) => fo.value.map(m -> _) }

    // Filter out all the "None" portfolio prices.
    // So as to not have options as values.
    pricesO.map(p =>
      Map(p.toSeq: _*).collect {
        case (k, Some(v)) => (k, v)
    })
  }

  /**
    * Snapshot of the initial margins when setting up the CCP.
    */
  private val initialMargins: Future[Map[ActorRef, BigDecimal]] = {
    val margins =
      if (rules.ccpRules.participatesInMargin) {
        for {
          (member, portfolio) <- allPortfolios
        } yield member -> portfolio.margin(zero)(rules.marginCoverage, rules.timeHorizon)
      } else {
        for {
          (member, portfolio) <- memberPortfolios
        } yield member -> portfolio.margin(zero)(rules.marginCoverage, rules.timeHorizon)
      }

    val marginsO = Future.traverse(margins) { case (m, fo) => fo.value.map(m -> _) }

    // Filter out all the "None" margins.
    // So as to not have options as values
    marginsO.map(m =>
      Map(m.toSeq: _*).collect {
        case (k, Some(v)) => (k, v)
    })
  }

  /**
    * Posted margins of members.
    */
  private var margins: Future[Map[ActorRef, BigDecimal]] = initialMargins

  /**
    * Snapshot of the default funds when setting up the CCP.
    */
  private val initDefaultFunds: Future[Map[ActorRef, BigDecimal]] = {
    def fundContribution(m: ActorRef) = {
      val funds = for {
        im <- OptionT(initialMargins.map(_.get(m)))
        f = im * rules.fundParticipation
      } yield f

      m -> funds
    }

    val funds = allMembers.map(fundContribution).toMap

    val fundsO = Future.traverse(funds) { case (m, fo) => fo.value.map(m -> _) }

    fundsO.map(f =>
      Map(f.toSeq: _*).collect {
        case (k, Some(v)) => (k, v)
    })
  }

//  logger.debug(s"Initial margins: $initialMargins")

  /**
    * Posted default funds of members.
    */
  private var defaultFunds: Future[Map[ActorRef, BigDecimal]] = initDefaultFunds

  /**
    * How much unfunded funds can still be called upon.
    */
  private var unfundedFundsLeft: Map[ActorRef, BigDecimal] =
    allMembers.map(m => m -> rules.maximumFundCall)(breakOut)

  /**
    * Computes the percentage of the initial default funds of the member ex defaulted members.
    *
    * @param member member for which to compute the pro-rata
    * @return percentage of the default funds of the member ex defaulted members.
    */
  private def proRataDefaultFunds(member: ActorRef): OptionT[Future, BigDecimal] = {
    val survivingMembersFunds =
      initDefaultFunds.map(_.withFilter(e => !defaultedMembers.contains(e._1)))

    val totalFundsF = survivingMembersFunds.map(_.map(_._2).sum)

    for {
      part <- OptionT(initDefaultFunds.map(_.get(member)))
      totalFunds <- OptionT.liftF(totalFundsF)
      proRata <- OptionT.fromOption[Future](Try(part / totalFunds).toOption)
    } yield proRata
  }

  /**
    * Computes the percentage of the initial margins of the member ex defaulted members.
    *
    * @param member member for which to compute the pro-rata
    * @return percentage of the initial margins of the member ex defaulted members.
    */
  private def proRataInitialMargins(
      member: ActorRef
  ): OptionT[Future, BigDecimal] = {
    val survivingMembersMargins =
      initialMargins.map(_.withFilter(e => !defaultedMembers.contains(e._1)))

    val totalMarginsF = survivingMembersMargins.map(_.map(_._2).sum)

    for {
      part <- OptionT(initialMargins.map(_.get(member)))
      totalMargins <- OptionT.liftF(totalMarginsF)
      proRata <- OptionT.fromOption[Future](Try(part / totalMargins).toOption)
    } yield proRata
  }

  /**
    * Expected payments after calls.
    */
  private var expectedPayments: Map[(ActorRef, RequestId), BigDecimal] = Map.empty

//  /**
//    * Expected default fund payment after fund call.
//    */
//  private var expectedDefaultFundPayments: Map[(ActorRef, RequestId), BigDecimal] = Map.empty

  private case class UnfundedBuffer(collected: BigDecimal,
                                    waitingFor: Int,
                                    defaultedMember: ActorRef,
                                    cost: BigDecimal)

  private var expectedUnfundedFunds: Map[RequestId, UnfundedBuffer] = Map.empty

  /**
    * Triggers margin calls at time t.
    * @param t time of margin call.
    */
  private def triggerMarginCalls(t: Time): Unit = {
//    implicit val timeout: Timeout = Timeout(60 seconds)

    val survivors = allMembers -- defaultedMembers
    survivors.foreach(memberMarginCall(_, t))

    previousCallTime = t
  }

  /**
    * Margin call to member m at time t.
    * @param member member to call.
    * @param t time of call.
    */
  private def memberMarginCall(member: ActorRef, t: Time): Unit =
    for {
      oldPrice <- OptionT(portfolioPrices(previousCallTime).map(_.get(member)))
      currentPrice <- OptionT(portfolioPrices(t).map(_.get(member)))

      variationMargin = oldPrice - currentPrice

      previousMargin <- OptionT(margins.map(_.get(member)))

      _ = logger.debug(s"Margin: $previousMargin")

      currentMargin = previousMargin - variationMargin

      // Update margin
      _ = self ! UpdateMargin(member, currentMargin)

      initialMargin <- OptionT(initialMargins.map(_.get(member)))

      marginCall = initialMargin - currentMargin // (initialMargin - currentMargin) + variationMargin

      // Also send negative margin calls (send money to the member).
      if marginCall.abs >= rules.minimumTransfer

      id = generateUuid

      _ = if (holdPayments && marginCall < 0) {
        for {
          currentDue <- paymentsDue.get(member)
        } yield self ! AddPaymentDue(member, currentDue + marginCall)
      } else {
        self ! AddExpectedPayment(member, id, marginCall)
        scheduleMessage(t, member, MarginCall(id, marginCall, rules.maxCallPeriod))
      }
    } yield ()

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
  ): Unit =
    for {
      // Update margin with payment
      currentMargin <- OptionT(margins.map(_.get(member)))
      _ = self ! UpdateMargin(member, currentMargin + payment)

      expectedPayment <- OptionT.fromOption[Future](expectedPayments.get((member, id)))
      _ = RemoveExpectedPayment(member, id)

      // Defaulted on payment
      if payment < expectedPayment
      paymentLeft = expectedPayment - payment
    } yield triggerDefault(member, paymentLeft, t)

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
  ): Unit =
    for {
      // Update fund with payment
      currentDefaultFund <- OptionT(defaultFunds.map(_.get(member)))
      _ = self ! UpdateFund(member, currentDefaultFund + payment)

      expectedPayment <- OptionT.fromOption[Future](expectedPayments.get((member, id)))
      _ = self ! RemoveExpectedPayment(member, id)

      // Defaulted on payment
      if payment < expectedPayment
      paymentLeft = expectedPayment - payment
    } yield triggerDefault(member, paymentLeft, t)

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
      waterfallId: RequestId,
      payment: BigDecimal,
      t: Time
  ): Unit =
    for {
      expectedPayment <- expectedPayments.get((member, id))
      _ = self ! RemoveExpectedPayment(member, id)

      b <- expectedUnfundedFunds.get(waterfallId)
      _ = expectedUnfundedFunds -= waterfallId

      _ = if (payment < expectedPayment) triggerDefault(member, expectedPayment - payment, t)

      _ = if (b.waitingFor == 1) {
        // Finished waiting
        scheduleMessage(t,
                        self,
                        CoverWithUnfunded(b.defaultedMember, b.cost, b.collected + payment))
      } else {

        // Update and wait
        expectedUnfundedFunds += waterfallId -> b.copy(collected = b.collected + payment,
                                                       waitingFor = b.waitingFor - 1)
      }
    } yield ()

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
    logger.debug("Started")

    // Margin payments are not paid during waterfalls if VMGH is used.
    haircutsInProgress += 1

    if (defaultedMembers.contains(member)) {
      nextStage(Start)(member, loss, t)
    } else {
      self ! AddDefaulted(member)

      for {
        portfolio <- OptionT.fromOption[Future](allPortfolios.get(member))
        replacementCost <- portfolio.replacementCost(t + portfolio.liquidity)

        // Waterfall for loss due incomplete call payment.
        _ = nextStage(Start)(member, loss, t)

        // Waterfall for loss due to replacement cost.
        _ = nextStage(Start)(member, replacementCost, t + portfolio.liquidity)
      } yield ()
    }
  }

  /**
    * Chooses what needs to be done after all the waterfall steps have been exhausted.
    * @param loss losses left.
    * @param time time
    */
  private def handleWaterfallResult(member: ActorRef, loss: BigDecimal, time: Time): Unit = {
    logger.debug("Ended")
    logger.debug(s"Loss $loss for member $member")
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
      Kleisli[OptionT[Future, ?], BigDecimal, BigDecimal](loss => {
        if (loss <= 0) {
          OptionT.fromOption[Future](Some(0))
        } else {
          // Left after using margin
          val lossAfterMarginUse = for {
            currentMargin <- OptionT(margins.map(_.get(defaultedMember)))

            // Cannot use negative margin
            lossLeft = loss - (currentMargin max 0)

            // Update margins, cannot use more than the current margin
            _ = self ! UpdateMargin(defaultedMember, currentMargin - (loss min currentMargin))
          } yield lossLeft

          lossAfterMarginUse.map(_ max 0)
        }
      })

    /**
      * Covers the losses with the defaulting member' posted default fund contribution.
      * @param defaultedMember the member that has to cover the losses
      * @return covered losses
      */
    def coverWithFund(defaultedMember: ActorRef) =
      Kleisli[OptionT[Future, ?], BigDecimal, BigDecimal](loss => {
        if (loss <= 0) {
          OptionT.fromOption[Future](Some(0))
        } else {
          // Left after funds use
          val lossAfterFundUse = for {
            currentFund <- OptionT(defaultFunds.map(_.get(defaultedMember)))

            // Cannot use negative fund
            lossLeft = loss - (currentFund max 0)

            _ = self ! UpdateFund(defaultedMember, currentFund - (loss min currentFund))
          } yield lossLeft

          lossAfterFundUse.map(_ max 0)
        }
      })

    val coverWithCollateral = coverWithInitialMargin(defaultedMember) andThen coverWithFund(
        defaultedMember)

    val delayedT = t + (delays.coverWithDefaultedMargin max delays.coverWithDefaultedFund)

    for {
      l <- coverWithCollateral(loss)
    } yield nextStage(Defaulted)(defaultedMember, l, delayedT)
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
      Kleisli[Future, BigDecimal, BigDecimal](loss => {
        if (loss <= 0) {
          Future.successful(BigDecimal(0))
        } else {
          val survivingMemberMargins =
            margins.map(_.withFilter(entry => !defaultedMembers.contains(entry._1)))

          val totalMarginsF = survivingMemberMargins.map(_.map(_._2).sum)

          val survivingMembers = allMembers -- defaultedMembers

          survivingMembers.foreach(
            member => {
              for {
                currentMargin <- OptionT(margins.map(_.get(member)))
                proRata <- proRataInitialMargins(member)
                totalMargins <- OptionT.liftF(totalMarginsF)
                payment = (loss min totalMargins) * proRata

                // Needs to pay
                if payment > 0

                _ = self ! UpdateMargin(member, currentMargin - payment)

                id = generateUuid
                _ = self ! AddExpectedPayment(member, id, payment)
              } yield
                scheduleMessage(t + delays.coverWithSurvivorsFunds,
                                member,
                                MarginCall(id, payment, rules.maxCallPeriod))
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
      Kleisli[Future, BigDecimal, BigDecimal](loss => {
        if (loss <= 0) {
          Future.successful(BigDecimal(0))
        } else {
          val survivingMemberFunds =
            defaultFunds.map(_.withFilter(entry => !defaultedMembers.contains(entry._1)))
          val totalFundsF = survivingMemberFunds.map(_.map(_._2).sum)

          val survivingMembers = allMembers -- defaultedMembers

          survivingMembers.foreach(
            member => {
              for {
                currentFund <- OptionT(defaultFunds.map(_.get(member)))
                proRata <- proRataDefaultFunds(member)
                totalFunds <- OptionT.liftF(totalFundsF)
                payment = (loss min totalFunds) * proRata

                // Needs to pay
                if payment > 0

                _ = self ! UpdateFund(member, currentFund - payment)

                id = generateUuid
                _ = self ! AddExpectedPayment(member, id, payment)
              } yield
                scheduleMessage(t + delays.coverWithSurvivorsFunds,
                                member,
                                DefaultFundCall(id, payment, rules.maxCallPeriod))
            }
          )

          totalFundsF.map(totalFunds => (loss - totalFunds) max 0)
        }
      })

    val coverWithSurvivors = coverWithNonDefaultingFunds(defaultedMember) andThen
        coverWithSurvivingMargin(defaultedMember)

    val delayedT = t + (delays.coverWithSurvivorsMargins max delays.coverWithSurvivorsFunds)

    for {
      l <- coverWithSurvivors(loss)
    } yield nextStage(Survivors)(defaultedMember, l, delayedT)
  }

  /**
    * Collects the unfunded funds in preparation for using it to cover the losses.
    * @param defaultedMember member that defaulted.
    * @param loss losses to cover.
    * @param t time
    */
  private def collectUnfunded(defaultedMember: ActorRef, loss: BigDecimal, t: Time): Unit = {
    //      implicit val timeout: Timeout = Timeout(60 seconds)
    val survivingMember = allMembers -- defaultedMembers
    val waterfallId = generateUuid

    val payments = survivingMember.map(member => {
      for {
        fundLeft <- OptionT.fromOption[Future](unfundedFundsLeft.get(member))
        proRata <- proRataDefaultFunds(member)

        // Don't call more than allowed
        payment = (loss * proRata) min fundLeft

        // Has to pay
        if payment > 0

        id = generateUuid
        _ = self ! AddExpectedPayment(member, id, payment)

        _ = scheduleMessage(
          t + delays.computeSurvivorsUnfunded,
          member,
          UnfundedDefaultFundCall(id, waterfallId, payment, rules.maxRecapPeriod))

        _ = self ! UpdateUnfunded(member, fundLeft - payment)
      } yield payment
    })

    expectedUnfundedFunds += waterfallId -> UnfundedBuffer(0, payments.size, defaultedMember, loss)
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

    nextStage(Unfunded)(defaultedMember, lossLeft, delayedT)
  }

  private def coverWithVMGH(defaultedMember: ActorRef, loss: BigDecimal, t: Time): Unit = {
    val totalDue = paymentsDue.values.sum

    val haircuts = paymentsDue.map({
      case (m, due) => (m, loss * (due / totalDue))
    })

    val collected = (for {
      (m, due) <- paymentsDue
      haircut <- haircuts.get(m)

      // Cannot cut more than is due
      afterHaircut = (due - haircut) max 0
      _ = paymentsDue += m -> afterHaircut
    } yield afterHaircut).sum

    nextStage(VMGH)(defaultedMember, loss - collected, t + delays.coverWithVMGH)
  }

  /**
    * Releases the variation margins due if no haircut is in process.
    * @param t time to release.
    */
  private def tryReleasePayments(t: Time): Unit = {
    haircutsInProgress -= 1

    // Can release the payments due
    if (haircutsInProgress == 0) {
      paymentsDue.foreach({
        case (m, due) =>
          scheduleMessage(t, m, MarginCall(generateUuid, due, rules.maxCallPeriod))
      })
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
    val totalEquity = _assets.values.sum
    val equityForFirstLevel = totalEquity * rules.skinInTheGame

    // Don't use more than the equity for the first level.
    val u =
      handlePayment(_assets, loss min equityForFirstLevel, delays.coverWithFirstLevelEquity)

    update(u.assets, u.timedPayment.payment)

    nextStage(FirstLevelEquity)(defaultedMember,
                                loss - u.timedPayment.payment,
                                t + u.timedPayment.delay)
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
    val u = handlePayment(_assets, loss, delays.coverWithSecondLevelEquity)

    update(u.assets, u.timedPayment.payment)

    nextStage(SecondLevelEquity)(defaultedMember,
                                 loss - u.timedPayment.payment,
                                 t + u.timedPayment.delay)
  }

  /**
    * Generates a unique id.
    * @return the unique id
    */
  private def generateUuid = new RequestId(UUID.randomUUID().toString)

  /**
    * Updates the member with its new assets and what has been paid for the call.
    * @param assets new assets.
    * @param payment additional payment.
    */
  private def update(assets: Map[Time, BigDecimal], payment: BigDecimal): Unit = {
    _assets = assets
    totalPaid += payment
  }

  /**
    * Updates the assets with the payment. Liquidity is assumed to be 0.
    * @param payment payment to add.
    */
  private def updateAssets(payment: BigDecimal): Unit = {
    for {
      currentAmount <- _assets.get(zero)

      _ = _assets += zero -> (currentAmount + payment)
    } yield ()
  }

  /**
    * Schedules a message to arrive a certain point in time in the future.
    * @param time arrival time of the message.
    * @param to recipient of the message.
    * @param message the message to be sent.
    */
  private def scheduleMessage(time: Time, to: ActorRef, message: Any): Unit =
    scheduler ! scheduledMessage(time, to, message)

  /**
    * Continues the waterfall with the next stage.
    * It shortcircuits the waterfall is there is no loss left.
    * @param currentStage the stage that is being currently run.
    * @param member the member that has defaulted.
    * @param loss the loss left to be covered.
    * @param t time at which to continue.
    */
  private def nextStage(
      currentStage: WaterfallStage)(member: ActorRef, loss: BigDecimal, t: Time): Unit = {
    assert(loss >= 0, s"Too much has been used during stage $currentStage")

    def toInternalStage(s: WaterfallStage)(member: ActorRef, loss: BigDecimal): InternalStage =
      s match {
        case Defaulted => CoverWithDefaulted(member, loss)
        case Survivors => CoverWithSurvivors(member, loss)
        case Unfunded => CollectUnfunded(member, loss)
        case VMGH => CoverWithVMGH(member, loss)
        case FirstLevelEquity => CoverWithFirstLevelEquity(member, loss)
        case SecondLevelEquity => CoverWithSecondLevelEquity(member, loss)
        case End => WaterfallResult(member, loss)
      }

    if (loss > 0) {
      // No need to keep payments if VMGH is done.
      if (currentStage == VMGH) tryReleasePayments(t)

      for {
        s <- waterfall.next(currentStage)
        message = toInternalStage(s)(member, loss)
      } yield scheduleMessage(t, self, message)
    } else {
      // Nothing left to do... shortcircuit the waterfall.
      tryReleasePayments(t)
      scheduleMessage(t, self, WaterfallResult(member, loss))
    }
  }
}

object Ccp {
  def props[A](name: String,
               waterfall: Waterfall,
               memberPortfolios: Map[ActorRef, Portfolio],
               ccpPortfolios: => Map[ActorRef, Portfolio],
               assets: Map[Time, BigDecimal],
               rules: Rules,
               operationalDelays: OperationalDelays,
               scheduler: ActorRef): Props = {
    Props(
      new Ccp(name,
              waterfall,
              memberPortfolios,
              ccpPortfolios,
              assets,
              rules,
              operationalDelays,
              scheduler))
  }

  /**
    * Rules of the CCP.
    *
    * @param marginIsRingFenced initial margin of non-defaulting members can be used
    * @param maximumFundCall maximum unfunded funds that can be called upon
    * @param ccpRules rules for CCPs
    */
  case class Rules(callEvery: Time,
                   maxCallPeriod: Time,
                   maxRecapPeriod: Time,
                   minimumTransfer: BigDecimal,
                   marginIsRingFenced: Boolean,
                   maximumFundCall: BigDecimal,
                   skinInTheGame: BigDecimal,
                   marginCoverage: BigDecimal,
                   timeHorizon: Time,
                   fundParticipation: BigDecimal,
                   ccpRules: CcpRules) {
    require(maximumFundCall >= 0)
    require(minimumTransfer >= 0)
  }

  case class CcpRules(participatesInMargin: Boolean)

  case class OperationalDelays(
      callHandling: Time,
      coverWithDefaultedMargin: Time,
      coverWithDefaultedFund: Time,
      coverWithSurvivorsMargins: Time,
      coverWithSurvivorsFunds: Time,
      computeSurvivorsUnfunded: Time,
      coverWithSurvivorsUnfunded: Time,
      coverWithVMGH: Time,
      coverWithFirstLevelEquity: Time,
      coverWithSecondLevelEquity: Time
  )

  case class Run(timeHorizon: Time)

  private trait InternalStage
  private case class CoverWithDefaulted(member: ActorRef, loss: BigDecimal) extends InternalStage
  private case class CoverWithSurvivors(member: ActorRef, loss: BigDecimal) extends InternalStage
  private case class CollectUnfunded(member: ActorRef, loss: BigDecimal) extends InternalStage
  private case class CoverWithUnfunded(member: ActorRef, loss: BigDecimal, collected: BigDecimal)
      extends InternalStage
  private case class CoverWithVMGH(member: ActorRef, loss: BigDecimal) extends InternalStage
  private case class CoverWithFirstLevelEquity(member: ActorRef, loss: BigDecimal)
      extends InternalStage
  private case class CoverWithSecondLevelEquity(member: ActorRef, loss: BigDecimal)
      extends InternalStage
  private case class WaterfallResult(member: ActorRef, loss: BigDecimal) extends InternalStage

  private case class AddExpectedPayment(member: ActorRef, id: RequestId, payment: BigDecimal)
  private case class RemoveExpectedPayment(member: ActorRef, id: RequestId)
  private case class UpdateMargin(member: ActorRef, margin: BigDecimal)
  private case class UpdateFund(member: ActorRef, fund: BigDecimal)
  private case class UpdateUnfunded(member: ActorRef, unfund: BigDecimal)
  private case class AddDefaulted(member: ActorRef)
  private case class AddPaymentDue(member: ActorRef, payment: BigDecimal)
}
