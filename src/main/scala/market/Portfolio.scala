//import scalaz.std.list._
//import scalaz.std.option._
//import scalaz.syntax.traverse._
import breeze.stats.distributions.Gaussian
import cats.instances.all._
import cats.syntax.traverse._

/**
 * Created by dennis on 6/10/16.
 */
case class Portfolio[A](positions: List[Position[A]])(implicit market: Market[A]) {
  private val cumStdNorm = (p: Double) => BigDecimal(Gaussian(0, 1).icdf(p))

  lazy val inverse: Portfolio[A] = Portfolio.inverse(this)

  // TODO what is price??
  def price: Option[BigDecimal] = {
    positions.map {
      case Position(item, amount) =>
        for {
          price <- market.price(item)
        } yield price * amount
    }.sequence[Option, BigDecimal].map(_.sum)
  }

  def shock(shock: BigDecimal): Unit = {
    for {
      position <- positions
    } market.shockItem(position.item, shock)
  }

  def margin(coverage: BigDecimal): Option[BigDecimal] =
    (for (p <- positions) yield {
      positionMargin(p.item, p.amount, coverage)
    }).sequence[Option, BigDecimal].map(_.sum)

  def replacementCost: Option[BigDecimal] =
    positions.map {
      case Position(item, amount) if amount > 0 =>
        for {
          price <- market.price(item)
        } yield price * amount
      case Position(item, amount) if amount <= 0 => Some(BigDecimal(0))
    }.sequence[Option, BigDecimal].map(_.sum)

  def positionMargin(position: A, n: Long, coverage: BigDecimal): Option[BigDecimal] =
    for {
      returnDistr <- market.returnDistr(position)
      price <- market.price(position)


      mu = BigDecimal(returnDistr.mu)
      sigma = BigDecimal(returnDistr.sigma)

      zScore = cumStdNorm(coverage.doubleValue)


      worstCaseReturn = {
        if (n >= 0) mu - sigma * zScore // Lowest return in confidence interval
        else mu + sigma * zScore // Highest return in confidence interval
      }

      // Margin per position
      margin = price * worstCaseReturn.abs

    } yield margin * n.abs

  private val implMarket: Market[A] = market
}

object Portfolio {
  def inverse[A](portfolio: Portfolio[A]): Portfolio[A] = {
    val inversePositions = for {
      position <- portfolio.positions
    } yield {
      position match {
        case Position(item, amount) => Position(item, -amount)
      }
    }

    Portfolio(inversePositions)(portfolio.implMarket)
  }
}

case class Position[A](item: A, amount: Long)
