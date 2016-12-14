package market

import structure.Timed.Time
import util.DataUtil.sumList

import scala.collection.breakOut

/**
  * Created by dennis on 6/10/16.
  */
case class Portfolio[A](positions: Map[A, Int], liquidity: Time)(implicit market: Market[A]) {
//  require(price.getOrElse(BigDecimal(42)) > 0, "Price of portfolio must be positive.")

  lazy val inverse: Portfolio[A] = Portfolio.inverse(this)

  def weights(time: Time): Map[A, BigDecimal] = {
    positions.flatMap {
      case (item, amount) =>
        for {
          price <- market.price(time)(item)
          total <- this.price(time)
          weight = (price * amount) / total
        } yield item -> weight
    }
  }

  def price(time: Time): Option[BigDecimal] =

    sumList(positions.map {
      case (item, amount) =>
        for {
          price <- market.price(time)(item)
        } yield price * amount
    }(breakOut))

//  def shock(shock: BigDecimal): Unit = {
//    for {
//      (item, _) <- positions
//    } yield market.shockItem(item, shock)
//  }

  def margin(time: Time)(coverage: BigDecimal, timeHorizon: Time): Option[BigDecimal] =
    if (isEmpty) Some(BigDecimal(0))
    else market.margin(time)(this, coverage, timeHorizon)

  def replacementCost(time: Time): Option[BigDecimal] =
    sumList(positions.map {
      case (item, amount) if amount > 0 =>
        for {
          price <- market.price(time)(item)
        } yield price * amount

      case (_, amount) if amount <= 0 =>
        Some(BigDecimal(0))
    }(breakOut))

  val isEmpty: Boolean = positions.isEmpty

  def isShort(time: Time): Option[Boolean] = price(time).map(_ < 0)

  private val implMarket: Market[A] = market
}

object Portfolio {
  def inverse[A](portfolio: Portfolio[A]): Portfolio[A] = {
    Portfolio(portfolio.positions map {
      case (item, amount) => item -> -amount
    }, portfolio.liquidity)(portfolio.implMarket)
  }
}
