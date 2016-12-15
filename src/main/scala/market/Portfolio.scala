package market

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import cats.data.OptionT
import cats.implicits._
import market.Market.{Margin, Price}
import structure.Timed.Time
import util.DataUtil.sumList

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Portfolio of positions.
  * @param positions instruments and their respective amount.
  * @param liquidity time to liquidate.
  * @param market market where the positions are traded.
  */
case class Portfolio(positions: Map[Security, Int], liquidity: Time, market: ActorRef) {
  implicit val timeout: Timeout = 60 seconds
  lazy val inverse: Portfolio = Portfolio.inverse(this)

  /**
    * Weights of each position at time t.
    * @param t time
    * @return the weights of each position at time t.
    */
  def weights(t: Time): Future[Map[Security, BigDecimal]] = {
    val listF = positions
      .map({
        case (item, amount) => {
          for {
            price <- OptionT((market ? Price(item, t)).mapTo[Option[BigDecimal]])
            total <- this.price(t)
            weight = (price * amount) / total
          } yield item -> weight
        }
      })
      .map(_.value)
      .toList
      .sequence[Future, Option[(Security, BigDecimal)]]
      .map(_.flatten)

    listF.map(l => Map(l: _*))
  }

  /**
    * Price of the portfolio at time t.
    * @param t time.
    * @return the price of the portfolio at time t.
    */
  def price(t: Time): OptionT[Future, BigDecimal] = {
    val prices = positions.map {
      case (item, amount) =>
        for {
          price <- OptionT((market ? Price(item, t)).mapTo[Option[BigDecimal]])
        } yield price * amount
    }.map(_.value).toList

    val pricesF = Future.sequence(prices)

    OptionT(pricesF.map(sumList(_)))
  }

  /**
    * Margin for the given coverage and timeHorizon.
    * @param t time
    * @param coverage value at risk.
    * @param timeHorizon time horizon for the value at risk.
    * @return the margin for the given coverage and timeHorizon
    */
  def margin(t: Time)(coverage: BigDecimal, timeHorizon: Time): OptionT[Future, BigDecimal] = {
    if (isEmpty) OptionT.fromOption[Future](Some(BigDecimal(0)))
    else OptionT((market ? Margin(this, coverage, timeHorizon, t)).mapTo[Option[BigDecimal]])
  }

  /**
    * Replacement cost of the portfolio at time t.
    * In a sense, the cost of the long positions
    * as they have to be replaced. Short positions do not have to be replaced.
    * @param t
    * @return
    */
  def replacementCost(t: Time): OptionT[Future, BigDecimal] = {
    val costs = positions.map {
      case (item, amount) if amount > 0 =>
        for {
          price <- OptionT((market ? Price(item, t)).mapTo[Option[BigDecimal]])
        } yield price * amount

      case (_, amount) if amount <= 0 =>
        OptionT.fromOption[Future](Some(BigDecimal(0)))
    }.map(_.value).toList

    val costsF = Future.sequence(costs)

    OptionT(costsF.map(sumList(_)))
  }

  val isEmpty: Boolean = positions.isEmpty

  def isShort(time: Time): OptionT[Future, Boolean] = price(time).map(_ < 0)
}

object Portfolio {
  def inverse(portfolio: Portfolio): Portfolio = {
    Portfolio(portfolio.positions map {
      case (item, amount) => item -> -amount
    }, portfolio.liquidity, portfolio.market)
  }
}
