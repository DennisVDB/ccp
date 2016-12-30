package market

import java.io.File

import akka.actor.{Actor, Props}
import akka.pattern.pipe
import breeze.linalg.{Axis, DenseVector}
import breeze.stats.distributions.{Gaussian, MultivariateGaussian}
import com.github.tototoshi.csv.CSVWriter
import com.typesafe.scalalogging.Logger
import market.Market.{Index, Margin, Price}
import spire.implicits._
import spire.math
import structure.Timed.{Time, res, _}
import util.DataUtil.ec
import util.Result.Result

import scala.concurrent.Future
//import scala.concurrent.ExecutionContext.Implicits.globalimport scala.concurrent.Future
import scala.concurrent.duration._
import scalaz.Scalaz._
import scalaz._

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
                  data: Option[Map[Time, Map[Security, BigDecimal]]])
    extends Actor {
  private val icdf = (p: Double) => BigDecimal(Gaussian(0, 1).icdf(p))

  private var generatedPrices = Map(FiniteDuration(0, res) -> prices)

  private val logger = Logger("Market")

  private val f = new File("out.csv")

  def receive: Receive = {
    case Price(i, t) => sender ! price(i, t)
    case Margin(portfolio, coverage, timeHorizon, t) =>
      margin(portfolio, coverage, timeHorizon, t).run pipeTo sender
  }

  /**
    * Provide the price for the instrument at the given time.
    * @param time time for price.
    * @param instrument instrument
    * @return the price for the instrument at the given time.
    */
  private def price(instrument: Security, time: Time): \/[String, BigDecimal] = {

    /**
      * Helper function for generating prices for a specific point in time.
      * Will also generate prices for the missing past.
      * @param t point in time of prices.
      * @return prices for point in time t.
      */
    def generatePrices(t: Time): Option[Map[Security, BigDecimal]] = data match {
      case Some(d) => d.get(t)

      case None =>
        val returns = retDistr.draw().map(BigDecimal(_))

        val newPsO = indexes.keys.toList
          .traverse[Option, (Security, BigDecimal)](security =>
            for {
              // If the data does not exist, recursively generate it.
              ps <- generatedPrices.get(t - tick).orElse(generatePrices(t - tick))
              p <- ps.get(security)
              i <- indexes.get(security)
              r = returns(i)
            } yield {
              // Store the generated past data.
              // Might just overwrite the same if it already existed.
              generatedPrices += (t - tick) -> ps

              // Price cannot be negative
              security -> ((p * (1 + (r / scaling))) max 0)
          })
          .map(_.toMap)

        // Store the new generated data.
        newPsO.foreach(newPs => generatedPrices += t -> newPs)

        for {
          newPs <- newPsO
        } {
          Future {
            val writer = CSVWriter.open(f, append = true)
            val stringifiedTime = t.toUnit(res).toString
            val stringifiedPs = newPs.values.map(_.toString)
            val row = Iterable(stringifiedTime) ++ stringifiedPs
            writer.writeRow(row.toSeq)
            writer.close()
          }
        }

        newPsO
    }

    val price = generatedPrices.get(time) match {
      case Some(ps) =>
        ps.get(instrument)
      case None =>
        for {
          ps <- generatePrices(time)
          p <- ps.get(instrument)
        } yield p
    }

    price \/> s"Could not get price for $instrument @$time"
  }

  /**
    * Provides the margin requirements for the portfolio.
    * It is assumed that the return distribution does not change over time.
    * @param t time at which the margin has to be computed.
    * @param portfolio portfolio to collateralize.
    * @param coverage coverage needed.
    * @return Amount of margin needed.
    */
  private def margin(portfolio: Portfolio,
                     coverage: BigDecimal,
                     timeHorizon: Time,
                     t: Time): Result[BigDecimal] = {
    logger.debug(s"Computing margin of portfolio $portfolio")

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

//        _ = logger.debug(s"$portfolio -> $pMuHorizon +- $pSigmaHorizon * $zScore")
//        _ = logger.debug(s"$portfolio -> $ret * $price")

        valueAtRisk = price * ret

        // Only take margin if the value at risk is negative. For instance it could be
        m = -valueAtRisk max 0

//        _ = logger.debug(s"m $m")
      } yield m // valueAtRisk is negative, we flip the sign.
    }
  }
}

object Market {
  type Index = Int

  case class Price(security: Security, t: Time)
  case class Margin(portfolio: Portfolio, coverage: BigDecimal, timeHorizon: Time, t: Time)

  def props(prices: Map[Security, BigDecimal],
            indexes: Map[Security, Index],
            retDistr: MultivariateGaussian,
            scaling: Int,
            data: Option[Map[Time, Map[Security, BigDecimal]]]): Props =
    Props(Market(prices, indexes, retDistr, scaling, data))
}
