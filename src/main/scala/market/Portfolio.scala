package market

import com.typesafe.scalalogging.Logger
import market.Portfolio.price
import structure.Timed.{Time, zero}
import util.Result.Result

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try
import scalaz.Scalaz._
import scalaz._
import spire.implicits._

import scala.math.{max, pow}

/**
  * Portfolio of positions.
  * @param positions instruments and their respective amount.
  */
case class Portfolio(positions: Map[Security, BigDecimal],
                     market: Market,
                     liquidityLoss: Int = 1,
                     illiquidityFactor: Double = 0) {
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
            p <- market.price(item, t)
            total <- price(this)(t)
            weight = Try((p * amount) / total).getOrElse(BigDecimal(1))
          } yield item -> weight
        }
      }
      .map(_.toMap)
  }

  /**
    * Margin for the given coverage and timeHorizon.
    * @param t time
    * @param coverage value at risk.
    * @return the margin for the given coverage and timeHorizon
    */
  def margin(t: Time)(coverage: BigDecimal): Result[BigDecimal] = {
    if (isEmpty) \/-(0)
    else
      for {
        price <- Portfolio.price(this)(t)
        margin <- market.margin(this, coverage, closeoutPeriod(price), t)
      } yield margin
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
    logger.debug("YOHO")
    val costF = positions.toList.map {
      case (item, amount) if amount < 0 =>
        for {
          currentPrice <- market.price(item, t)
          deltaT = closeoutPeriod(amount).toMinutes * liquidityLoss * (1 + illiquidityFactor)

//          _ = logger.debug(
//            s"NORMAL ${closeoutPeriod(amount).toMinutes * liquidityLoss} -- $deltaT")

          price <- market.price(item, t + (deltaT minutes))
        } yield (price - currentPrice) * amount

      case (_, amount) if amount >= 0 =>
        \/-(BigDecimal(0))
    }.suml

    val timeToCloseOut = positions.map {
      case (_, amount) if amount < 0 =>
        val t = closeoutPeriod(amount).toMinutes * liquidityLoss * (1 + illiquidityFactor)
        t minutes
      case _ => zero
    }.max

    costF.map(cost => (cost, timeToCloseOut))
  }

  def closeoutPeriod(amount: BigDecimal): Time = {
    val t = (pow(amount.abs.doubleValue(), 1.1) / 64) //  * (1 + illiquidityFactor)

//    logger.debug(s"CLOSEOUT $t")

//    max(t, 15 * (1 + illiquidityFactor)) minutes
      max(t, 15) minutes
  }

  val isEmpty: Boolean = positions.isEmpty

  def isShort(t: Time): Result[Boolean] = price(this)(t).map(_ > 0)

  def /(n: Int): Portfolio = {
    val newPos = positions.mapValues(_ / n)
    copy(positions = newPos)
  }
}

object Portfolio {
  val logger = Logger("portfolio")

  type TradeAction =
    Portfolio => (Security, BigDecimal, Time) => Result[Transaction]

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
    p.positions.toList.map {
      case (item, amount) =>
        for {
          price <- p.market.price(item, t)
        } yield price * amount
    }.suml
  }

  def perform(p: Portfolio)(security: Security, cash: BigDecimal, t: Time)(
      f: (BigDecimal, BigDecimal) => BigDecimal) = {
    for {
      currentAmount <- p.positions.get(security) \/> s"Could not get position for security $security."

      wasLong = currentAmount >= 0

      performTime = t + p.closeoutPeriod(cash)

      price <- p.market
        .price(security, performTime)
        .ensure(s"Price is zero of $security.")(_ != 0)

      newAmount = if (wasLong) {
        f(currentAmount, cash / price) max 0 // Cannot sell more than you have
      } else {
        f(currentAmount, cash / price) min 0
      }

      raised = ((currentAmount - newAmount) * price).abs

      newPortfolio = p.copy(positions = p.positions + (security -> newAmount))
    } yield Transaction(newPortfolio, raised, p.closeoutPeriod(cash))
  }

  def performAll(p: Portfolio)(cash: BigDecimal, t: Time)(
      f: TradeAction): Result[Transaction] = {
    val totalPriceF = price(p)(t + p.closeoutPeriod(cash))

    val update = (security: Security) =>
      (work: Result[Transaction]) =>
        for {
          Transaction(p, currentRaised, currentTimeToPerform) <- work

          performTime = t + p.closeoutPeriod(cash)

          price <- p.market.price(security, performTime)

          amount <- p.positions.get(security) \/> s"Could not get position for security $security."

          totalPrice <- totalPriceF

          Transaction(updatedPortfolio, raised, timeToPerform) <- if (totalPrice != 0) {
            f(p)(security, cash * ((price * amount) / totalPrice), t)
          } else {
            f(p)(security, cash, t)
          }
        } yield
          Transaction(updatedPortfolio,
                      currentRaised + raised,
                      currentTimeToPerform max timeToPerform)

    val securities = p.positions.keys.toList

    val _performAll = for {
      _ <- securities
        .traverse[State[Result[Transaction], ?], Unit](
          State.modify[Result[Transaction]] _ compose update)
      s <- State.get
    } yield s

    _performAll.run(\/-(Transaction(p, 0, zero)))._2
  }

  def sell(p: Portfolio)(security: Security,
                         amount: BigDecimal,
                         t: Time): Result[Transaction] =
    perform(p)(security, amount, t) { _ - _ }

  def sellAll(p: Portfolio)(cash: BigDecimal, t: Time): Result[Transaction] =
    performAll(p)(cash, t)(sell)

  def buy(p: Portfolio)(security: Security,
                        amount: BigDecimal,
                        t: Time): Result[Transaction] =
    perform(p)(security, amount, t) { _ + _ }

  def buyAll(p: Portfolio)(cash: BigDecimal, t: Time): Result[Transaction] =
    performAll(p)(cash, t)(buy)

  case class Transaction(portfolio: Portfolio,
                         transactionAmount: BigDecimal,
                         timeToPerform: Time)

  implicit val portfolioSemigroup: Semigroup[Portfolio] =
    new Semigroup[Portfolio] {
      def append(f1: Portfolio, f2: => Portfolio): Portfolio = {
        assert(f1.market == f2.market)
        Portfolio(f1.positions |+| f2.positions,
                  f1.market,
                  max(f1.liquidityLoss, f2.liquidityLoss),
                  max(f1.illiquidityFactor, f2.illiquidityFactor))
      }
    }
}
