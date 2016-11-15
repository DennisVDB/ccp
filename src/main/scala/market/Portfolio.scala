//import scalaz.std.list._
//import scalaz.std.option._
//import scalaz.syntax.traverse._
import DataUtil._
import breeze.stats.distributions.Gaussian

import scala.collection.breakOut

/**
  * Created by dennis on 6/10/16.
  */
case class Portfolio[A](positions: Map[A, Long])(implicit market: Market[A]) {
  require(price.getOrElse(BigDecimal(42)) > 0, "Price of portfolio must be positive.")

  private val cumStdNorm = (p: Double) => BigDecimal(Gaussian(0, 1).icdf(p))

  lazy val inverse: Portfolio[A] = Portfolio.inverse(this)

  def weights: Map[A, BigDecimal] = {
    positions.flatMap {
      case (item, amount) =>
        for {
          price <- market.price(item)
          total <- this.price
          if total > 0 // Only long porfolios

          weight = (price * amount) / total
        } yield item -> weight
    }
  }

  def price: Option[BigDecimal] =
    sumList(positions.map {
      case (item, amount) =>
        for {
          price <- market.price(item)
        } yield price * amount
    }(breakOut))

  def shock(shock: BigDecimal): Unit = {
    for {
      (item, _) <- positions
    } yield market.shockItem(item, shock)
  }

  def margin(coverage: BigDecimal): Option[BigDecimal] =
    if (isEmpty) Some(BigDecimal(0))
    else market.margin(this, coverage)

  def replacementCost: Option[BigDecimal] =
    sumList(positions.map {
      case (item, amount) if amount > 0 =>
        for {
          price <- market.price(item)
        } yield price * amount

      case (item, amount) if amount <= 0 =>
        Some(BigDecimal(0))
    }(breakOut))

//  def positionMargin(position: A,
//                     n: Long,
//                     coverage: BigDecimal): Option[BigDecimal] =
//    market.margin(Portfolio(Map(position -> n)), coverage)

  val isEmpty: Boolean = positions.isEmpty

  private val implMarket: Market[A] = market
}

object Portfolio {
  def inverse[A](portfolio: Portfolio[A]): Portfolio[A] = {
    Portfolio(portfolio.positions map {
      case (item, amount) => item -> -amount
    })(portfolio.implMarket)
  }
}
