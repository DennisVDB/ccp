package market

import java.io.File

import breeze.linalg.{Axis, DenseVector}
import breeze.stats.distributions.{Gaussian, MultivariateGaussian}
import cats.implicits._
import com.github.tototoshi.csv.CSVWriter
import com.typesafe.scalalogging.Logger
import market.Market.{Index, Price}
import spire.implicits._
import spire.math
import structure.Timed.{Time, _}

import scala.collection._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
/**
  * Provides the prices for any point in time.
  *
  * @param prices the prices at time 0.
  * @param indexes the indexes of the instruments in the covariance matrix.
  * @param retDistr the probability distribution of the instruments' prices.
  * @tparam A type of the instruments.
  */
case class Market[A](prices: Map[A, Price],
                     indexes: Map[A, Index],
                     retDistr: MultivariateGaussian,
                     scaling: Int) {
  private val icdf = (p: Double) => BigDecimal(Gaussian(0, 1).icdf(p))

  private val generatedPrices = concurrent.TrieMap(FiniteDuration(0, res) -> prices)

  private val logger = Logger("Market")

  private val f = new File("out.csv")

  /**
    * Provide the price for the instrument at the given time.
    * @param time time for price.
    * @param instrument instrument
    * @return the price for the instrument at the given time.
    */
  def price(time: Time)(instrument: A): Option[Price] = {

    /**
      * Helper function for generating prices for a specific point in time.
      * Will also generate prices for the missing past.
      * @param t point in time of prices.
      * @return prices for point in time t.
      */
    def generatePrices(t: Time): Option[Map[A, Price]] = {
      val rs = retDistr.draw().map(BigDecimal(_))

      val newPs = indexes.keys
        .map(a =>
          for {
            // If the data does not exist, recursively generate it.
            ps <- generatedPrices.get(t - tick).orElse(generatePrices(t - tick))

            p <- ps.get(a)
            i <- indexes.get(a)
            r = rs(i)
          } yield {
            // Store the generated past data.
            // Might just overwrite the same if it already existed.
            generatedPrices.put(t - tick, ps)

            // Price cannot be negative
            a -> ((p * (1 + (r / scaling))) max 0)
        })
        .toList
        .sequence[Option, (A, Price)]
        .map(_.toMap)

      // Store the new generated data.
      newPs.map(generatedPrices.put(t, _))

      for {
        ps <- newPs
      } {
        Future {
          val writer = CSVWriter.open(f, append = true)
          val stringifiedTime = t.toUnit(res).toString
          val stringifiedPs = ps.values.map(_.toString)
          val row = Iterable(stringifiedTime) ++ stringifiedPs
          writer.writeRow(row.toSeq)
          writer.close()
        }
      }

      newPs
    }

    generatedPrices.get(time) match {
      case Some(ps) =>
        ps.get(instrument)
      case None =>
        for {
          ps <- generatePrices(time)
          p <- ps.get(instrument)
        } yield p
    }
  }

  /**
    * Provides the margin requirements for the portfolio.
    * It is assumed that the return distribution does not change over time.
    * @param time time at which the margin has to be computed.
    * @param portfolio portfolio to collateralize.
    * @param coverage coverage needed.
    * @return Amount of margin needed.
    */
  def margin(time: Time)(portfolio: Portfolio[A],
                         coverage: BigDecimal,
                         timeHorizon: Time): Option[BigDecimal] = {
    val foo = timeHorizon.toUnit(res)

    // Set of positions in the portfolio.
    val pPositions = portfolio.positions.keys.toSet

    // Indexes for each portfolio position.
    val pIndexes = indexes.filterKeys(pPositions)

    // Indexes of instruments that are not part of the portfolio.
    val indexesToStrip = indexes.filterNot(i => pIndexes.contains(i._1)).values

    if (pIndexes.size != portfolio.positions.size)
      None // Market does not contain all portfolio elements.
    else {
      for {
        price <- portfolio.price(time)

        pWeights = portfolio.weights(time).values

        weightsVec = DenseVector(pWeights.toList: _*).map(_.doubleValue).asDenseMatrix

        // Covariance matrix stripped from the instruments not in the portfolio.
        cov = retDistr.covariance
          .delete(indexesToStrip.toSeq, Axis._0)
          .delete(indexesToStrip.toSeq, Axis._1)

        pVar = (weightsVec.t * cov * weightsVec).map(BigDecimal(_) / scaling)

        // pVar should be a matrix of one element.
        if pVar.rows == 1 && pVar.cols == 1

        pSigma = math.sqrt(pVar(0, 0))

        // Volatility over horizon
        pSigmaHorizon = pSigma * math.sqrt(BigDecimal(timeHorizon.toUnit(res)))

        // Z-value for the given coverage.
        zScore = icdf(coverage.doubleValue)

        // Market mean vector stripped from the instruments not in the portfolio.
        muVec = retDistr.mean.toDenseMatrix.delete(indexesToStrip.toSeq, Axis._1).toDenseVector

        pMu = BigDecimal((muVec :* weightsVec.toDenseVector).toArray.sum) / scaling

        // Return over horizon
        pMuHorizon = ((1 + pMu) ** timeHorizon.toUnit(res)) - 1

        // If base portfolio is long we take lower bound as we need to cover from a price drop.
        // We do the opposite for a short portfolio.
        // We can do this as the returns of the portfolio are normally distributed.
        ret = if (price >= 0) pMuHorizon - pSigmaHorizon * zScore
        else pMuHorizon + pSigmaHorizon * zScore

        _ = logger.debug(s"$portfolio -> $pMuHorizon +- $pSigmaHorizon * $zScore")
        _ = logger.debug(s"$portfolio -> $ret * $price")

        valueAtRisk = price * ret

        // Only take margin if the value at risk is negative. For instance it could be
        margin = -valueAtRisk max 0
      } yield margin // valueAtRisk is negative, we flip the sign.
    }
  }
}

object Market {
  type Price = BigDecimal
  type Index = Int
}
