package structure

import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import cats.data.{Kleisli, OptionT}
import cats.implicits._
import com.typesafe.scalalogging.Logger
import market.Portfolio
import structure.Ccp.{Run, _}
import structure.Scheduler.{TriggerMarginCalls, scheduledMessage}
import structure.Timed._
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
    memberPortfolios: Map[ActorRef, Portfolio],
    ccpPortfolios: => Map[ActorRef, Portfolio],
    assets: Map[Time, BigDecimal],
    rules: Rules,
    delays: OperationalDelays,
    scheduler: ActorRef
) extends Actor
    with PaymentSystem {
  override def receive: Receive = {
    case Run(timeHorizon: Time) =>
//      scheduleMessage(60, self, TriggerMarginCalls)
      scheduleMarginCalls(zero)

//      scheduleMessage(timeHorizon + 1, self, Paid)

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
                            MarginCallResponse(self, id, u.timedPayment.payment))

          }

        case DefaultFundCall(id, payment, maxDelay) =>
          val u = handlePayment(_assets, payment, maxDelay - delays.callHandling)
          update(u.assets, u.timedPayment.payment)
          scheduleMessage(t + u.timedPayment.delay + delays.callHandling,
                          sender,
                          DefaultFundCallResponse(self, id, u.timedPayment.payment))

        case UnfundedDefaultFundCall(id, waterfallId, payment, maxDelay) =>
          val u = handlePayment(_assets, payment, maxDelay - delays.callHandling)
          update(u.assets, u.timedPayment.payment)
          scheduleMessage(
            t + u.timedPayment.delay + delays.callHandling,
            sender,
            UnfundedDefaultFundCallResponse(self, id, waterfallId, u.timedPayment.payment))

        /* Responses */
        case MarginCallResponse(responder, id, payment) =>
          handleMarginCallResponse(responder, id, payment, t)

        case DefaultFundCallResponse(responder, id, payment) =>
          handleDefaultFundCallResponse(responder, id, payment, t)

        case UnfundedDefaultFundCallResponse(responder, id, waterfallId, payment) =>
          handleUnfundedDefaultFundCallResponse(responder, id, waterfallId, payment, t)

        /* Waterfall */
        case CoverWithDefaulted(member, cost) =>
          coverWithDefaultedCollateral(member, cost, t)

        case CoverWithSurvivors(member, cost) =>
          coverWithNonDefaultedCollateral(member, cost, t)

        case CollectUnfundedFunds(member, cost) =>
          collectUnfundedFunds(member, cost, t)

        case CoverWithSurvivorsUnfunded(member, cost, collected) =>
          coverWithNonDefaultingUnfundedFunds(member, cost, collected, t)

        case CoverWithFirstLevelEquity(member, cost) =>
          coverWithFirstLevelEquity(member, cost, t)

        case CoverWithSecondLevelEquity(member, cost) =>
          coverWithSecondLevelEquity(member, cost, t)

        case WaterfallResult(costLeft) => handleWaterfallResult(costLeft, t)
      }
  }

  private val logger = Logger(name)

  private var currentTime = zero
  private var previousCallTime = zero

  private var _assets = assets
  private var totalPaid: BigDecimal = 0

  private val members: Set[ActorRef] = memberPortfolios.keys.toSet
  private val ccps: Set[ActorRef] = ccpPortfolios.keys.toSet
  private val allMembers: Set[ActorRef] = members ++ ccps

  private val allPortfolios: Map[ActorRef, Portfolio] = memberPortfolios ++ ccpPortfolios

  private var defaultedMembers: Set[ActorRef] = Set.empty

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

  logger.debug(s"Initial margins: $initialMargins")

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
  private def proRataDefaultFunds(member: ActorRef): Future[Option[BigDecimal]] = {
    val survivingMembersFunds =
      initDefaultFunds.map(_.withFilter(e => !defaultedMembers.contains(e._1)))

    val totalFundsF = survivingMembersFunds.map(_.map(_._2).sum)

    (for {
      part <- OptionT(initDefaultFunds.map(_.get(member)))
      totalFunds <- OptionT.liftF(totalFundsF)
      proRata <- OptionT.fromOption[Future](Try(part / totalFunds).toOption)
    } yield proRata).value
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
    * Expected margin payment after margin call.
    */
  private var expectedMarginPayments: Map[(ActorRef, RequestId), BigDecimal] = Map.empty

  /**
    * Expected default fund payment after fund call.
    */
  private var expectedDefaultFundPayments: Map[(ActorRef, RequestId), BigDecimal] = Map.empty

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

      margin <- OptionT(margins.map(_.get(member)))

      // Update member's margin account
      _ = margins = margins.map(m => m.+((member, margin - variationMargin)))

//      _ = margins += (member -> (margin - variationMargin))

      initialMargin <- OptionT(initialMargins.map(_.get(member)))

      // Amount below initial margin ...
      // (happens when previously
      // the variation margin was too small
      // to trigger a margin call)
      // ... plus the variation margin.
      marginCall = (initialMargin - margin) + variationMargin

      // Also send negative margin calls (send money to the member).
      if marginCall.abs >= rules.minimumTransfer

      id = generateUuid

      // Only needs a response if a payment is needed.
      _ = if (marginCall >= 0) {
        // Store the request in order to be able to check the response.
        expectedMarginPayments += (member, id) -> marginCall
      }
    } yield {
      logger.debug(s"Sending $marginCall to $member")
      scheduleMessage(t, member, MarginCall(id, marginCall, rules.maxCallPeriod))
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
  ): Unit =
    for {
      // Update margin with payment
      currentMargin <- OptionT(margins.map(_.get(member)))
      _ = margins = margins.map(m => m.+((member, currentMargin + payment)))

      expectedPayment <- OptionT.fromOption[Future](expectedMarginPayments.get((member, id)))
      _ = expectedMarginPayments -= ((member, id))

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
      _ = defaultFunds = defaultFunds.map(m => m.+((member, currentDefaultFund + payment)))

      expectedPayment <- OptionT.fromOption[Future](expectedDefaultFundPayments.get((member, id)))
      _ = expectedDefaultFundPayments -= ((member, id))

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
      expectedPayment <- expectedDefaultFundPayments.get((member, id))
      _ = expectedDefaultFundPayments -= ((member, id))

      b <- expectedUnfundedFunds.get(waterfallId)
      _ = expectedUnfundedFunds -= waterfallId

      _ = if (payment < expectedPayment) triggerDefault(member, expectedPayment - payment, t)

      _ = if (b.waitingFor == 1) {
        // Finished waiting
        scheduleMessage(
          t,
          self,
          CoverWithSurvivorsUnfunded(b.defaultedMember, b.cost, b.collected + payment))
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
    if (defaultedMembers.contains(member)) {
      scheduleMessage(t, self, CoverWithDefaulted(member, loss))
    } else {
      defaultedMembers += member
      for {
        portfolio <- OptionT.fromOption[Future](allPortfolios.get(member))
        replacementCost <- portfolio.replacementCost(t + portfolio.liquidity)
      } yield {
        // Waterfall for loss due incomplete call payment.
        scheduleMessage(t, self, CoverWithDefaulted(member, loss))

        // Waterfall for loss due to replacement cost.
        scheduleMessage(t + portfolio.liquidity, self, CoverWithDefaulted(member, replacementCost))
      }
    }
  }

  /**
    * Chooses what needs to be done after all the waterfall steps have been exhausted.
    * @param loss losses left.
    * @param time time
    */
  private def handleWaterfallResult(loss: Option[BigDecimal], time: Time): Unit = {
    loss match {
      case Some(l) =>
        if (l == 0) logger.debug("SUCCESS")
        else logger.debug("FAILURE")
      case None => logger.debug("WATERFALL ERROR!")
    }
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
            _ = margins =
              margins.map(m => m.+((defaultedMember, currentMargin - (loss min currentMargin))))
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

            _ = defaultFunds =
              defaultFunds.map(m => m.+((defaultedMember, currentFund - (loss min currentFund))))
          } yield lossLeft

          lossAfterFundUse.map(_ max 0)
        }
      })

    val coverWithCollateral = coverWithInitialMargin(defaultedMember) andThen coverWithFund(
        defaultedMember)

    val delayedT = t + (delays.coverWithDefaultedMargin max delays.coverWithDefaultedFund)

    coverWithCollateral(loss).value.foreach {
      case Some(lossLeft) =>
        scheduleMessage(delayedT, self, CoverWithFirstLevelEquity(defaultedMember, lossLeft))
      case None => scheduleMessage(delayedT, self, WaterfallResult(None))
    }
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

                _ = margins = margins.map(m => m.+((member, currentMargin - payment)))

                id = generateUuid
                _ = expectedMarginPayments += (member, id) -> payment
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
                proRata <- OptionT(proRataDefaultFunds(member))
                totalFunds <- OptionT.liftF(totalFundsF)
                payment = (loss min totalFunds) * proRata

                // Needs to pay
                if payment > 0

                _ = defaultFunds = defaultFunds.map(m => m.+((member, currentFund - payment)))

                id = generateUuid
                _ = expectedDefaultFundPayments += (member, id) -> payment
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

    val lossLeftF = coverWithSurvivors(loss)

    lossLeftF.foreach(
      lossLeft =>
        scheduleMessage(t + (delays.coverWithSurvivorsMargins max delays.coverWithSurvivorsFunds),
                        self,
                        CollectUnfundedFunds(defaultedMember, lossLeft)))
  }

  /**
    * Collects the unfunded funds in preparation for using it to cover the losses.
    * @param defaultedMember member that defaulted.
    * @param loss losses to cover.
    * @param t time
    */
  private def collectUnfundedFunds(defaultedMember: ActorRef, loss: BigDecimal, t: Time): Unit = {
    if (loss <= 0) {
      scheduleMessage(t, self, WaterfallResult(Some(0)))
    } else {
      //      implicit val timeout: Timeout = Timeout(60 seconds)
      val survivingMember = allMembers -- defaultedMembers
      val waterfallId = generateUuid

      val payments = survivingMember.map(member => {
        for {
          fundLeft <- OptionT.fromOption[Future](unfundedFundsLeft.get(member))
          proRata <- OptionT(proRataDefaultFunds(member))

          // Don't call more than allowed
          payment = (loss * proRata) min fundLeft

          // Has to pay
          if payment > 0

          id = generateUuid
          _ = expectedDefaultFundPayments += (member, id) -> payment

          _ = scheduleMessage(
            t + delays.computeSurvivorsUnfunded,
            member,
            UnfundedDefaultFundCall(id, waterfallId, payment, rules.maxRecapPeriod))

          _ = unfundedFundsLeft += member -> (fundLeft - payment)
        } yield payment
      })

      expectedUnfundedFunds += waterfallId -> UnfundedBuffer(0,
                                                             payments.size,
                                                             defaultedMember,
                                                             loss)
    }
  }

  /**
    * Covers the losses with the collected unfunded funds.
    * @param defaultedMember member that defaulted.
    * @param loss losses to cover.
    * @param collected amount collected.
    * @param t time
    */
  private def coverWithNonDefaultingUnfundedFunds(defaultedMember: ActorRef,
                                                  loss: BigDecimal,
                                                  collected: BigDecimal,
                                                  t: Time): Unit = {
    val lossLeft = loss - collected

    val delayedT = t + delays.coverWithSurvivorsUnfunded

    if (lossLeft <= 0)
      scheduleMessage(delayedT, self, WaterfallResult(Some(0)))
    else
      scheduleMessage(delayedT, self, CoverWithSecondLevelEquity(defaultedMember, lossLeft))
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
    if (loss <= 0) {
      scheduleMessage(t, self, WaterfallResult(Some(0)))
    } else {
      val totalEquity = _assets.values.sum
      val equityForFirstLevel = totalEquity * rules.skinInTheGame

      // Don't use more than the equity for the first level.
      val u =
        handlePayment(_assets, loss min equityForFirstLevel, delays.coverWithFirstLevelEquity)

      update(u.assets, u.timedPayment.payment)

      scheduleMessage(t + u.timedPayment.delay,
                      self,
                      CoverWithSurvivors(defaultedMember, loss - u.timedPayment.payment))
    }
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
    if (loss <= 0) {
      scheduleMessage(t, self, WaterfallResult(Some(0)))
    } else {
      val u = handlePayment(_assets, loss, delays.coverWithSecondLevelEquity)

      update(u.assets, u.timedPayment.payment)

      scheduleMessage(t + u.timedPayment.delay,
                      self,
                      WaterfallResult(Some((loss - u.timedPayment.payment) max 0)))
    }
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
  private def update(assets: Map[Time, BigDecimal], payment: BigDecimal) = {
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

  private def scheduleMessage(time: Time, to: ActorRef, message: Any) =
    scheduler ! scheduledMessage(time, to, message)
}

object Ccp {
  def props[A](name: String,
               memberPortfolios: Map[ActorRef, Portfolio],
               ccpPortfolios: => Map[ActorRef, Portfolio],
               assets: Map[Time, BigDecimal],
               rules: Rules,
               operationalDelays: OperationalDelays,
               scheduler: ActorRef): Props = {
    Props(
      new Ccp(name, memberPortfolios, ccpPortfolios, assets, rules, operationalDelays, scheduler))
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
      coverWithFirstLevelEquity: Time,
      coverWithSecondLevelEquity: Time
  )

  case class Run(timeHorizon: Time)
}
