package structure.ccp

import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import akka.pattern.pipe
import com.typesafe.scalalogging.Logger
import market.Portfolio
import market.Portfolio.{buyAll, sellAll}
import structure.Scheduler.{ScheduledMessage, TriggerMarginCalls, scheduledMessage}
import structure.Timed._
import structure._
import structure.ccp.Ccp._
import structure.ccp.Waterfall.{Failed, _}
import util.Result
import util.Result.{Result, resultMonoid}

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
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
    name: String,
    waterfall: Waterfall,
    memberPortfolios: Map[ActorRef, Portfolio],
    ccpPortfolios: => Map[ActorRef, Portfolio],
    capital: Portfolio,
    rules: Rules,
    delays: OperationalDelays,
    market: ActorRef,
    scheduler: ActorRef
) extends Actor {
  override def receive: Receive = LoggingReceive {
    case Run(timeHorizon: Time) =>
      scheduleMarginCalls(zero)

      /**
        * Recursively schedules margin calls from time t up to the time horizon.
        *
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

    case TriggerDefault(member, paymentLeft, t) => triggerDefault(member, paymentLeft, t)

    case NextStage(currentStage, member, loss, t) => nextStage(currentStage)(member, loss, t)

    case FailedResult(member, _) => throw new IllegalStateException(s"Failed with $member")

    case TimedMessage(t, m) =>
      assert(t >= currentTime,
             "Received message from the past, time is +" + currentTime + ". " + m + " @" + t)
      currentTime = currentTime max t

      val currentCapital = _capital
      val origSender = sender

      m match {
        case Paid =>
        case TriggerMarginCalls => triggerMarginCalls(t)

        /* Calls */
        case Transfer(payment) =>
          require(payment >= 0)

          val newCapital = for {
            c <- currentCapital
            (newC, _) <- buyAll(c)(payment, t)
          } yield newC

          // Reinvestment not fail
          _capital = newCapital

        case MarginCall(id, payment, maxDelay) =>
          require(payment >= 0)

          val newCapitalAndTimeF = for {
            c <- currentCapital
            (newCap, timeToSell) <- sellAll(c)(payment, t)
            if timeToSell <= maxDelay
          } yield (newCap, timeToSell)

          val newCapitalF = newCapitalAndTimeF.map(_._1)
          val timeToSellF = newCapitalAndTimeF.map(_._2)

          // Fails if not enough was sold or on time, reassign same capital
          _capital = newCapitalF ||| currentCapital

          val response = ((timeToSellF | Timed.zero) |@| (timeToSellF.map(_ => payment) | 0))(
            (timeToSell, raisedAmount) => {
              scheduledMessage(t + timeToSell,
                               origSender,
                               MarginCallResponse(id, self, raisedAmount))
            })

          response pipeTo scheduler

        case DefaultFundCall(id, payment, maxDelay) =>
          require(payment >= 0)

          val newCapitalAndTimeF = for {
            c <- currentCapital
            (newCap, timeToSell) <- sellAll(c)(payment, t)
            if timeToSell <= maxDelay
          } yield (newCap, timeToSell)

          val newCapitalF = newCapitalAndTimeF.map(_._1)
          val timeToSellF = newCapitalAndTimeF.map(_._2)

          // Fails if not enough was sold or on time, reassign same capital
          _capital = newCapitalF ||| currentCapital

          val response = ((timeToSellF | Timed.zero) |@| (timeToSellF.map(_ => payment) | 0))(
            (timeToSell, raisedAmount) => {
              scheduledMessage(t + timeToSell,
                               origSender,
                               DefaultFundCallResponse(id, self, raisedAmount))
            })

          response pipeTo scheduler

        case UnfundedDefaultFundCall(id, waterfallId, payment, maxDelay) =>
          require(payment >= 0)

          val newCapitalAndTimeF = for {
            c <- currentCapital
            (newCap, timeToSell) <- sellAll(c)(payment, t)
            if timeToSell <= maxDelay
          } yield (newCap, timeToSell)

          val newCapitalF = newCapitalAndTimeF.map(_._1)
          val timeToSellF = newCapitalAndTimeF.map(_._2)

          // Fails if not enough was sold or on time, reassign same capital
          _capital = newCapitalF ||| currentCapital

          val response = ((timeToSellF | Timed.zero) |@| (timeToSellF
            .map(_ => payment) | 0))((timeToSell, raisedAmount) => {
            scheduledMessage(t + timeToSell,
                             origSender,
                             UnfundedDefaultFundCallResponse(id, waterfallId, self, raisedAmount))
          })

          response pipeTo scheduler

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

  private var currentTime = zero
  private var previousCallTime = zero

  private var _capital = Result.pure(capital)
  private var totalPaid: BigDecimal = 0

  private val members: Set[ActorRef] = memberPortfolios.keys.toSet
  private val ccps: Set[ActorRef] = ccpPortfolios.keys.toSet
  private val allMembers: Set[ActorRef] = members ++ ccps

  private val allPortfolios: Map[ActorRef, Portfolio] = memberPortfolios ++ ccpPortfolios

  private var defaultedMembers: Set[ActorRef] = Set.empty

  private var paymentsDue: Map[ActorRef, Result[BigDecimal]] = Map.empty
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
    Result.fromOptRes(for {
      p <- allPortfolios.get(member)
    } yield Portfolio.price(p)(t))
  }

  /**
    * Posted margins of members.
    */
  private var margins: Map[ActorRef, Result[BigDecimal]] = {
    if (rules.ccpRules.participatesInMargin) {
      for {
        (member, portfolio) <- allPortfolios
      } yield {
        member -> portfolio.margin(zero)(rules.marginCoverage, rules.timeHorizon)
      }
    } else {
      for {
        (member, portfolio) <- memberPortfolios
      } yield {
        member -> portfolio.margin(zero)(rules.marginCoverage, rules.timeHorizon)
      }
    }
  }

  /**
    * Snapshot of the initial margins when setting up the CCP.
    */
  private val initialMargins: Map[ActorRef, Result[BigDecimal]] = margins

  /**
    * Snapshot of the default funds when setting up the CCP.
    */
  private val initDefaultFunds: Future[Map[ActorRef, BigDecimal]] = {
    def fundContribution(m: ActorRef) = {
      val funds = for {
        im <- Result.fromOptRes(initialMargins.get(m))
        f = im * rules.fundParticipation
      } yield f

      m -> funds
    }

    val funds = allMembers.map(fundContribution).toMap

    val fundsO = Future.traverse(funds) { case (m, fo) => fo.run.map(m -> _) }

    fundsO.map(f =>
      Map(f.toSeq: _*).collect {
        case (k, Some(v)) => (k, v)
    })
  }

  /**
    * Posted default funds of members.
    */
  private var defaultFunds: Map[ActorRef, Result[BigDecimal]] = {
    def fundContribution(m: ActorRef) = {
      val funds = for {
        im <- Result.fromOptRes(initialMargins.get(m))
        f = im * rules.fundParticipation
      } yield f

      m -> funds
    }

    allMembers.map(fundContribution).toMap
  }

  /**
    * How much unfunded funds can still be called upon.
    */
  private var unfundedFundsLeft: Map[ActorRef, Result[BigDecimal]] =
    allMembers.map(_ -> Result.pure(rules.maximumFundCall)).toMap

  /**
    * Computes the percentage of the initial default funds of the member ex defaulted members.
    *
    * @param member member for which to compute the pro-rata
    * @return percentage of the default funds of the member ex defaulted members.
    */
  private def proRataDefaultFunds(member: ActorRef): Result[BigDecimal] = {
    val currentDefaulted = defaultedMembers

    val survivingMembersFunds =
      initDefaultFunds.map(_.withFilter(e => !currentDefaulted.contains(e._1)))

    val totalFundsF = survivingMembersFunds.map(_.map(_._2).sum)

    for {
      part <- Result(initDefaultFunds.map(_.get(member)))
      totalFunds <- Result.fromFuture(totalFundsF)
      if totalFunds != 0
    } yield part / totalFunds
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

    val survivingMembersMargins = initialMargins.withFilter(e => !currentDefaulted.contains(e._1))

    val totalMarginsF = survivingMembersMargins.map(_._2).toList.suml

    for {
      part <- Result.fromOptRes(initialMargins.get(member))
      totalMargins <- totalMarginsF
      if totalMargins != 0
    } yield part / totalMargins
  }

  /**
    * Expected payments after calls.
    */
  private var expectedPayments: Map[(ActorRef, RequestId), Result[BigDecimal]] = Map.empty

  private case class UnfundedBuffer(collected: BigDecimal,
                                    waitingFor: Int,
                                    defaultedMember: ActorRef,
                                    cost: BigDecimal)

  private var expectedUnfundedFunds: Map[RequestId, UnfundedBuffer] = Map.empty

  /**
    * Triggers margin calls at time t.
    *
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
    *
    * @param member member to call.
    * @param t      time of call.
    */
  private def memberMarginCall(member: ActorRef, t: Time): Future[ScheduledMessage] = {
    val lastCall = previousCallTime
    val previousMargins = margins

    val previousMarginF = Result.fromOptRes(previousMargins.get(member))
    val oldPriceF = portfolioPrice(member, lastCall)
    val currentPriceF = portfolioPrice(member, t)

    val currentMarginF =
      (previousMarginF |@| oldPriceF |@| currentPriceF) {
        (previousMargin, oldPrice, currentMargin) =>
          previousMargin - (oldPrice - currentMargin)
      }

    margins += member -> currentMarginF

    val marginCallF = for {
      initialMargin <- Result.fromOptRes(initialMargins.get(member))
      currentMargin <- currentMarginF

      marginCall = initialMargin - currentMargin

      // Also send negative margin calls (send money to the member).
      if marginCall.abs >= rules.minimumTransfer
    } yield marginCall

    val currentDues = paymentsDue
    val holdPayments = shouldHoldPayments

    val paymentDue = for {
      marginCall <- marginCallF
      if marginCall < 0 && holdPayments

      _ = logger.debug(s"VMGH $holdPayments")

      currentDue <- Result.fromOptRes(currentDues.get(member))
    } yield currentDue + marginCall

    paymentsDue += member -> paymentDue

    val id = generateUuid

    val outboundMarginCallF = marginCallF.filter(_ > 0 || !holdPayments)

    expectedPayments += (member, id) -> outboundMarginCallF

    val messageF = for {
      mc <- outboundMarginCallF
      message = if (mc > 0) {
        scheduledMessage(t, member, MarginCall(id, mc, rules.maxCallPeriod))
      } else {
        scheduledMessage(t, member, Transfer(-mc))
      }
    } yield message

    Result.collect(messageF) pipeTo scheduler
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
    val currentMargins = margins

    val newMargin = for {
      currentMargin <- Result.fromOptRes(currentMargins.get(member))
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
    val currentFunds = defaultFunds

    val updateFund = for {
      currentFund <- Result.fromOptRes(currentFunds.get(member))
    } yield currentFund + payment

    defaultFunds += member -> updateFund

    checkAndTrigger(member, id, payment, t)
  }

  private def checkAndTrigger(member: ActorRef, id: RequestId, payment: BigDecimal, t: Time) = {
    val paymentLeftMessageF = for {
      expectedPayment <- Result.fromOptRes(expectedPayments.get((member, id)))

      _ = logger.debug(s"$member $payment < $expectedPayment")

      // Defaulted on payment
      if payment < expectedPayment

      _ = logger.debug(s"$member defaulted")
    } yield TriggerDefault(member, expectedPayment - payment, t)

    expectedPayments -= ((member, id))

    Result.collect(paymentLeftMessageF) pipeTo self
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
      waterfallId: RequestId,
      payment: BigDecimal,
      t: Time
  ): Unit = {
    val triggerDefaultMessageF = for {
      expectedPayment <- Result.fromOptRes(expectedPayments.get((member, id)))
      if payment < expectedPayment
    } yield TriggerDefault(member, expectedPayment - payment, t)

    Result.collect(triggerDefaultMessageF) pipeTo self

    for {
      b <- expectedUnfundedFunds.get(waterfallId)

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

    expectedPayments -= ((member, id))
    expectedUnfundedFunds -= waterfallId
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
  ): Future[NextStage] = {

    // Margin payments are not paid during waterfalls if VMGH is used.
    haircutsInProgress += 1

    if (defaultedMembers.contains(member)) {
      throw new IllegalStateException()
    } else {
      defaultedMembers += member

      val cover = for {
        portfolio <- Result.fromOption(allPortfolios.get(member))
        (replacementCost, timeToReplace) <- portfolio.replacementCost(t)
      } yield
        (NextStage(Start, member, loss, t),
         NextStage(Start, member, replacementCost, t + timeToReplace))

      val coverMessages = Result.collect(cover)

      coverMessages.map(_._1) pipeTo self
      coverMessages.map(_._2) pipeTo self
    }
  }

  /**
    * Chooses what needs to be done after all the waterfall steps have been exhausted.
    * @param loss losses left.
    * @param time time
    */
  private def handleWaterfallResult(member: ActorRef, loss: BigDecimal, time: Time): Unit = {
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
                                           t: Time): Future[NextStage] = {

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

          val currentMarginF = Result.fromOptRes(currentMargins.get(defaultedMember))

          // Left after using margin
          val lossAfterMarginUse = for {
            currentMargin <- currentMarginF
          } yield loss - (currentMargin max 0)

          val newMargin = for {
            currentMargin <- currentMarginF
          } yield currentMargin - (loss min currentMargin)

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
          val currentFunds = defaultFunds

          val currentFundF = Result.fromOptRes(currentFunds.get(defaultedMember))

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

    val coverWithCollateral = coverWithInitialMargin(defaultedMember) andThen coverWithFund(
        defaultedMember)

    val delayedT = t + (delays.coverWithMargin max delays.coverWithFund)

    val currentMargins = margins
    val currentFunds = defaultFunds

    val margin = Result.fromOptRes(currentMargins.get(defaultedMember))
    val fund = Result.fromOptRes(currentFunds.get(defaultedMember))
    val id = generateUuid

    val transfer = (margin |@| fund) { (m, f) =>
      scheduledMessage(t, defaultedMember, Transfer(m + f))
    }

    Result.collect(transfer) pipeTo scheduler

    val nextStageMessage = for {
      l <- coverWithCollateral(loss)
    } yield NextStage(Defaulted, defaultedMember, l, delayedT)

    Result.collect(nextStageMessage) pipeTo self
  }

  /**
    * Covers the loss with the collateral posted by the surviving members.
    * @param defaultedMember member that defaulted
    * @param loss losses to cover
    * @param t time
    */
  private def coverWithNonDefaultedCollateral(defaultedMember: ActorRef,
                                              loss: BigDecimal,
                                              t: Time): Future[NextStage] = {

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
          val currentDefaulted = defaultedMembers
          val currentMargins = margins

          val survivingMemberMargins =
            currentMargins.withFilter(entry => !currentDefaulted.contains(entry._1))

          val foo = survivingMemberMargins.map(_._2).toList

          val totalMarginsF =
            survivingMemberMargins
              .map(_._2)
              .toList
              .suml

          val survivingMembers = allMembers -- currentDefaulted

          survivingMembers.foreach(
            member => {
              val currentMarginF = Result.fromOptRes(currentMargins.get(member))

              val paymentF = for {
                proRata <- proRataInitialMargins(member)
                totalMargins <- totalMarginsF
                payment = (loss min totalMargins) * proRata
                if payment > 0
              } yield payment

              val newMarginF = (currentMarginF |@| paymentF) { _ - _ }

              margins += defaultedMember -> (newMarginF ||| currentMarginF)

              val id = generateUuid

              val marginCallMessageF = for {
                payment <- paymentF
              } yield
                scheduledMessage(t + delays.coverWithSurvivorsMargins,
                                 member,
                                 MarginCall(id, payment, rules.maxCallPeriod))

              expectedPayments += (member, id) -> paymentF
              Result.collect(marginCallMessageF) pipeTo scheduler
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
          val currentDefaulted = defaultedMembers
          val currentFunds = defaultFunds

          val survivingMemberFunds =
            currentFunds.withFilter(entry => !currentDefaulted.contains(entry._1))

          val totalFundsF = survivingMemberFunds
            .map(_._2)
            .toList
            .suml

          val survivingMembers = allMembers -- currentDefaulted

          survivingMembers.foreach(
            member => {
              val currentFundF = Result.fromOptRes(currentFunds.get(member))

              val paymentF = for {
                proRata <- proRataDefaultFunds(member)
                totalFunds <- totalFundsF
                payment = (loss min totalFunds) * proRata
                if payment > 0
              } yield payment

              val newFundF = (currentFundF |@| paymentF) { _ - _ }

              defaultFunds += defaultedMember -> (newFundF ||| currentFundF)

              val id = generateUuid

              val fundCallMessageF = for {
                payment <- paymentF
              } yield
                scheduledMessage(t + delays.coverWithSurvivorsFunds,
                                 member,
                                 DefaultFundCall(id, payment, rules.maxRecapPeriod))

              expectedPayments += (member, id) -> paymentF
              Result.collect(fundCallMessageF) pipeTo scheduler
            }
          )

          totalFundsF.map(totalFunds => (loss - totalFunds) max 0)
        }
      })

    val coverWithSurvivors = coverWithNonDefaultingFunds(defaultedMember) andThen
        coverWithSurvivingMargin(defaultedMember)

    val delay =
      if (rules.marginIsRingFenced) delays.coverWithSurvivorsFunds
      else delays.coverWithMargin max delays.coverWithFund

    val delayedT = t + delay

    val nextStageMessageF = for {
      l <- coverWithSurvivors(loss)
    } yield NextStage(Survivors, defaultedMember, l, delayedT)

    (nextStageMessageF getOrElse NextStage(Failed, defaultedMember, 0, delayedT)) pipeTo self
  }

  /**
    * Collects the unfunded funds in preparation for using it to cover the losses.
    * @param defaultedMember member that defaulted.
    * @param loss losses to cover.
    * @param t time
    */
  private def collectUnfunded(defaultedMember: ActorRef, loss: BigDecimal, t: Time): Unit = {
    //      implicit val timeout: Timeout = Timeout(60 seconds)
    val survivingMembers = allMembers -- defaultedMembers
    val waterfallId = generateUuid
    val currentUnfundedLeft = unfundedFundsLeft

    survivingMembers.map { member =>
      val unfundedLeftF = Result.fromOptRes(currentUnfundedLeft.get(member))

      val paymentF = for {
        unfundedLeft <- unfundedLeftF
        proRata <- proRataDefaultFunds(member)

        // Don't call more than allowed
        payment = (loss * proRata) min unfundedLeft
        if payment > 0
      } yield payment

      val newFundLeftF = (unfundedLeftF |@| paymentF)(_ - _)
      unfundedFundsLeft += member -> (newFundLeftF ||| unfundedLeftF)

      val id = generateUuid

      val unfundedFundCallMessageF = for {
        payment <- paymentF
      } yield
        scheduledMessage(t + delays.computeSurvivorsUnfunded,
                         member,
                         UnfundedDefaultFundCall(id, waterfallId, payment, rules.maxCallPeriod))

      expectedPayments += (member, id) -> paymentF
      Result.collect(unfundedFundCallMessageF) pipeTo scheduler
    }

    expectedUnfundedFunds += waterfallId -> UnfundedBuffer(0,
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

    nextStage(Unfunded)(defaultedMember, lossLeft, delayedT)
  }

  private def coverWithVMGH(defaultedMember: ActorRef, loss: BigDecimal, t: Time): Unit = {
    val currentDues = paymentsDue

    val totalDue = currentDues.values.toList.suml

    val available = for {
      tot <- totalDue
    } yield loss min tot

    val haircuts = currentDues.map {
      case (m, due) =>
        (m, (due |@| totalDue |@| available)((d, tot, av) => av * (d / tot)))
    }

    val newDue = for {
      (m, due) <- currentDues
      haircut <- haircuts.get(m)

      // Cannot cut more than is due
      afterHaircut = ^(due, haircut)(_ - _)
    } yield m -> afterHaircut

    paymentsDue = newDue

    val nextStageMessageF = for {
      av <- available
    } yield NextStage(VMGH, defaultedMember, loss - av, t + delays.coverWithVMGH)

    Result.collect(nextStageMessageF) pipeTo self
  }

  /**
    * Releases the variation margins due if no haircut is in process.
    * @param t time to release.
    */
  private def tryReleasePayments(t: Time): Unit = {
    haircutsInProgress -= 1

    val currentDues = paymentsDue

    // Can release the payments due
    if (haircutsInProgress == 0) {
      val paymentDueMessagesF = currentDues.map {
        case (m, due) =>
          for {
            d <- due
          } yield scheduledMessage(t, m, MarginCall(generateUuid, d, rules.maxCallPeriod))
      }

      paymentDueMessagesF.map(Result.collect).map(_ pipeTo scheduler)
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
                                        t: Time): Future[NextStage] = {
    val currentCapital = _capital

    val totalEquityF = currentCapital.flatMap(Portfolio.price(_)(t))

    val equityForFirstLevelF = totalEquityF.map(_ * rules.skinInTheGame)

    val toSellF = equityForFirstLevelF.map(_ min loss)

    val newCapAndTime = for {
      c <- currentCapital
      toSell <- toSellF
      r <- sellAll(c)(toSell, t)
    } yield r

    val newCapitalF = newCapAndTime.map(_._1)
    val timeToSellF = newCapAndTime.map(_._2)

    // Fails if selling failed
    _capital = newCapitalF

    val nextStageMessage = for {
      timeToSell <- timeToSellF
      toSell <- toSellF
    } yield NextStage(FirstLevelEquity, defaultedMember, loss - toSell, t + timeToSell)

    Result.collect(nextStageMessage) pipeTo self
  }

  /**
    * Covers the losses with a certain percentage of the CCPs equity.
    * @param defaultedMember member that defaulted.
    * @param loss losses to cover.
    * @param t time
    */
  private def coverWithSecondLevelEquity(defaultedMember: ActorRef,
                                         loss: BigDecimal,
                                         t: Time): Future[NextStage] = {
    val currentCapital = _capital

    val totalEquityF = currentCapital.flatMap(Portfolio.price(_)(t))

    val toSellF = totalEquityF.map(_ min loss)

    val newCapAndTime = for {
      c <- currentCapital
      toSell <- toSellF
      r <- sellAll(c)(toSell, t)
    } yield r

    val newCapitalF = newCapAndTime.map(_._1)
    val timeToSellF = newCapAndTime.map(_._2)

    // Fails if selling failed
    _capital = newCapitalF

    val nextStageMessage = for {
      timeToSell <- timeToSellF
      toSell <- toSellF
    } yield NextStage(SecondLevelEquity, defaultedMember, loss - toSell, t + timeToSell)

    Result.collect(nextStageMessage) pipeTo self
  }

  /**
    * Generates a unique id.
    * @return the unique id
    */
  private def generateUuid = new RequestId(UUID.randomUUID().toString)

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

    logger.debug(s"Finished $currentStage with $loss @$t ($member)")

    def toInternalStage(s: WaterfallStage)(member: ActorRef, loss: BigDecimal): InternalStage =
      s match {
        case Start => throw new IllegalArgumentException()
        case Defaulted => CoverWithDefaulted(member, 1000)
        case Survivors => CoverWithSurvivors(member, 1000)
        case Unfunded => CollectUnfunded(member, 1000)
        case VMGH => CoverWithVMGH(member, 1000)
        case FirstLevelEquity => CoverWithFirstLevelEquity(member, 1000)
        case SecondLevelEquity => CoverWithSecondLevelEquity(member, 1000)
        case End => WaterfallResult(member, 1000)
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

      scheduler ! switchStateMessageO.getOrElse(throw new IllegalStateException())
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
               capital: Portfolio,
               rules: Rules,
               operationalDelays: OperationalDelays,
               market: ActorRef,
               scheduler: ActorRef): Props = {
    Props(
      new Ccp(name,
              waterfall,
              memberPortfolios,
              ccpPortfolios,
              capital,
              rules,
              operationalDelays,
              market,
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
  private case class FailedResult(member: ActorRef, loss: BigDecimal) extends InternalStage

  private case class AddExpectedPayment(member: ActorRef, id: RequestId, payment: BigDecimal)
  private case class RemoveExpectedPayment(member: ActorRef, id: RequestId)
  private case class UpdateFund(member: ActorRef, fund: BigDecimal)
  private case class UpdateUnfunded(member: ActorRef, unfund: BigDecimal)
  private case class AddDefaulted(member: ActorRef)
  private case class TriggerDefault(member: ActorRef, paymentLeft: BigDecimal, t: Time)
  private case class NextStage(currentStage: WaterfallStage,
                               member: ActorRef,
                               loss: BigDecimal,
                               t: Time)
}
