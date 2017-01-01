package market

import java.io.File

import breeze.linalg.{Axis, DenseVector}
import breeze.stats.distributions.{Gaussian, MultivariateGaussian}
import com.typesafe.scalalogging.Logger
import market.Market.Index
import spire.implicits._
import spire.math
import structure.Timed.{Time, res}
import util.Result.Result

import scalaz.Scalaz._

/**
  * Provides the prices for any point in time.
  *
  * @param prices the prices at time 0.
  * @param indexes the indexes of the instruments in the covariance matrix.
  * @param retDistr the probability distribution of the instruments' prices.
  */
case class Market(prices: Map[Security, BigDecimal],
                  indexes: Map[Security, Index],
                  retDistr: MultivariateGaussian,
                  scaling: Int,
                  data: Map[Time, Map[Security, BigDecimal]]) {
  private val icdf = (p: Double) => BigDecimal(Gaussian(0, 1).icdf(p))
  private val logger = Logger("Market")
  private val f = new File("out.csv")

  /**
    * Provide the price for the instrument at the given time.
    * @param t time for price.
    * @param instrument instrument
    * @return the price for the instrument at the given time.
    */
  def price(instrument: Security, t: Time): Result[BigDecimal] = {
    for {
      dataAtTime <- data.get(t) \/> s"Could not get data at time $t."
      price <- dataAtTime.get(instrument) \/> s"Could not get price for $instrument @$t"
    } yield price
  }

  /**
    * Provides the margin requirements for the portfolio.
    * It is assumed that the return distribution does not change over time.
    * @param t time at which the margin has to be computed.
    * @param portfolio portfolio to collateralize.
    * @param coverage coverage needed.
    * @return Amount of margin needed.
    */
  def margin(portfolio: Portfolio,
             coverage: BigDecimal,
             timeHorizon: Time,
             t: Time): Result[BigDecimal] = {
    // Set of positions in the portfolio.
    val pPositions = portfolio.positions.keys.toSet

    // Indexes for each portfolio position.
    val pIndexes = indexes.filterKeys(pPositions)

    // Indexes of instruments that are not part of the portfolio.
    val indexesToStrip = indexes.filterNot(i => pIndexes.contains(i._1)).values

    if (pIndexes.size != portfolio.positions.size)
      throw new IllegalArgumentException("Portfolio is broken.") // Market does not contain all portfolio elements.
    else {
      for {
        pWeights <- portfolio
          .weights(t)
          .map(
            _.toList
              .map {
                case (k, v) => (indexes.get(k).orElse(throw new IllegalStateException()), v)
              }
              .sortBy(_._1)
              .map(_._2))

//        _ = logger.debug(s"pWeights $pWeights")

        price <- Portfolio.price(portfolio)(t)

//        _ = logger.debug(s"price $price")

        weightsVec = DenseVector(pWeights: _*).map(_.doubleValue)

//        _ = logger.debug(s"weightsVec $weightsVec")

        // Covariance matrix stripped from the instruments not in the portfolio.
        cov = retDistr.covariance
          .delete(indexesToStrip.toSeq, Axis._0)
          .delete(indexesToStrip.toSeq, Axis._1)

//        _ = logger.debug(s"cov $cov")

        pVar = BigDecimal(weightsVec.t * cov * weightsVec) / scaling

        pSigma = math.sqrt(pVar)

        // Volatility over horizon
        pSigmaHorizon = pSigma * math.sqrt(BigDecimal(timeHorizon.toUnit(res)))

        // Z-value for the given coverage.
        zScore = icdf(coverage.doubleValue)

        // Market mean vector stripped from the instruments not in the portfolio.
        muVec = retDistr.mean.toDenseMatrix.delete(indexesToStrip.toSeq, Axis._1).toDenseVector

        pMu = BigDecimal(muVec.t * weightsVec) / scaling

//        _ = logger.debug(s"pMu $pMu")

//        _ = logger.debug(s"Horizon ${timeHorizon.toUnit(res)}")

        // Return over horizon
        pMuHorizon = ((1 + pMu) ** timeHorizon.toUnit(res)) - 1

        // If base portfolio is long we take lower bound as we need to cover from a price drop.
        // We do the opposite for a short portfolio.
        // We can do this as the returns of the portfolio are normally distributed.
        ret = if (price >= 0)
          (pMuHorizon - pSigmaHorizon * zScore) max -1 // Cannot lose more than 100% if long
        else pMuHorizon + pSigmaHorizon * zScore

//        _ = logger.debug(s"$pMuHorizon +- $pSigmaHorizon * $zScore")
//        _ = logger.debug(s"$ret * $price")

        valueAtRisk = price * ret

        // Only take margin if the value at risk is negative. For instance it could be
        m = -valueAtRisk max 0

//        _ = logger.debug(s"MARGIN $m")
      } yield m // valueAtRisk is negative, we flip the sign.
    }
  }
}

object Market {
  type Index = Int
}
