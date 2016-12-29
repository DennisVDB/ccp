package market

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import market.Market.{Margin, Price}
import market.Portfolio.price
import structure.Timed.{Time, zero}
import util.Result
import util.Result.{Result, resultMonoid}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scalaz.Scalaz._
import scalaz._

/**
  * Portfolio of positions.
  * @param positions instruments and their respective amount.
  */
case class Portfolio(positions: Map[Security, BigDecimal], market: ActorRef) {
  implicit val timeout: Timeout = 60 seconds
  lazy val inverse: Portfolio = Portfolio.inverse(this)
  private val logger = Logger("portfolio")

  /**
    * Weights of each position at time t.
    * @param t time
    * @return the weights of each position at time t.
    */
  def weights(t: Time): Result[Map[Security, BigDecimal]] = {
    positions.toList
      .traverse[Result, (Security, BigDecimal)] {
        case (item, amount) => {
          for {
            p <- Result((market ? Price(item, t)).mapTo[Option[BigDecimal]])
            total <- price(this)(t)
            weight = (p * amount) / total
          } yield item -> weight
        }
      }
      .map(_.toMap)
  }

  /**
    * Margin for the given coverage and timeHorizon.
    * @param t time
    * @param coverage value at risk.
    * @param timeHorizon time horizon for the value at risk.
    * @return the margin for the given coverage and timeHorizon
    */
  def margin(t: Time)(coverage: BigDecimal, timeHorizon: Time): Result[BigDecimal] = {
    if (isEmpty) Result.pure(0)
    else Result((market ? Margin(this, coverage, timeHorizon, t)).mapTo[Option[BigDecimal]])
  }

  /**
    * Replacement cost of the portfolio at time t.
    * In order to replace the long positions, the short position (held by the CCP)
    * has to be closed (pay market price), and then sell a new short position.
    * Replacing a long position is thus "free".
    * In order to replace the short positions, the long position (held by the CCP)
    * has to be closed (don't get anything, member defaulted). And buy a new long position.
    * Replacing the short position is the market price.
    * @param t
    * @return
    */
  def replacementCost(t: Time): Result[(BigDecimal, Time)] = {
    val costsF = positions.toList
      .traverse[Result, BigDecimal] {
        case (item, amount) if amount < 0 =>
          val price =
            Result((market ? Price(item, t + closeoutPeriod(amount))).mapTo[Option[BigDecimal]])
          price.map(-_ * amount)

        case (_, amount) if amount >= 0 =>
          Result.pure(0)
      }

    val timeToCloseOut = positions.map {
      case (_, amount) if amount < 0 => closeoutPeriod(amount)
    }.max

    costsF.map(costs => (costs.sum, timeToCloseOut))
  }

  def closeoutPeriod(amount: BigDecimal): Time = 15 minutes

  val isEmpty: Boolean = positions.isEmpty

  def isShort(t: Time): Result[Boolean] = price(this)(t).map(_ > 0)
}

object Portfolio {
  val logger = Logger("portfolio")

  implicit val timeout: Timeout = 60 seconds

  type TradeAction =
    Portfolio => (Security, BigDecimal, Time) => Result[(Portfolio, Time)]

  def inverse(p: Portfolio): Portfolio =
    p.copy(positions = p.positions map {
      case (item, amount) => item -> -amount
    })

  /**
    * Price of the portfolio at time t.
    * @param t time.
    * @return the price of the portfolio at time t.
    */
  def price(p: Portfolio)(t: Time): Result[BigDecimal] = {
    p.positions.toList
      .map({
        case (item, amount) =>
          for {
            price <- Result((p.market ? Price(item, t)).mapTo[Option[BigDecimal]])
          } yield price * amount
      })
      .suml
  }

  def perform(p: Portfolio)(security: Security, cash: BigDecimal, t: Time)(
      f: (BigDecimal, BigDecimal) => BigDecimal) = {
    for {
      currentAmount <- Result.fromOption(p.positions.get(security))

      wasLong = currentAmount >= 0

      performTime = t + p.closeoutPeriod(cash)

      price <- Result((p.market ? Market.Price(security, performTime)).mapTo[Option[BigDecimal]])
      if price != 0

      newAmount = f(currentAmount, cash / price)

      isLong = newAmount >= 0

      // Did not change from long to short, and inversely
      if wasLong == isLong

      newPortfolio = p.copy(positions = p.positions + (security -> newAmount))
    } yield (newPortfolio, p.closeoutPeriod(cash))
  }

  def performAll(p: Portfolio)(cash: BigDecimal, t: Time)(
      f: TradeAction): Result[(Portfolio, Time)] = {
    val totalPriceF = price(p)(t)

    val update = (security: Security) =>
      (work: Result[(Portfolio, Time)]) =>
        for {
          (p, currentTimeToPerform) <- work
          price <- Result((p.market ? Price(security, t)).mapTo[Option[BigDecimal]])
          amount <- Result.fromOption(p.positions.get(security))

          totalPrice <- totalPriceF

          (updatedPortfolio, timeToPerform) <- if (totalPrice != 0) {
            f(p)(security, cash * ((price * amount) / totalPrice), t)
          } else {
            f(p)(security, cash, t)
          }
        } yield (updatedPortfolio, currentTimeToPerform max timeToPerform)

    val securities = p.positions.keys.toList

    val _performAll = for {
      _ <- securities.traverse[State[Result[(Portfolio, Time)], ?], Unit](
        State.modify[Result[(Portfolio, Time)]] _ compose update)

      s <- State.get
    } yield s

    _performAll.run(Result.pure((p, zero)))._2
  }

  def sell(
      p: Portfolio)(security: Security, amount: BigDecimal, t: Time): Result[(Portfolio, Time)] =
    perform(p)(security, amount, t) { _ - _ }

  def sellAll(p: Portfolio)(cash: BigDecimal, t: Time): Result[(Portfolio, Time)] =
    performAll(p)(cash, t)(sell)

  def buy(
      p: Portfolio)(security: Security, amount: BigDecimal, t: Time): Result[(Portfolio, Time)] =
    perform(p)(security, amount, t) { _ + _ }

  def buyAll(p: Portfolio)(cash: BigDecimal, t: Time): Result[(Portfolio, Time)] =
    performAll(p)(cash, t)(buy)
}
