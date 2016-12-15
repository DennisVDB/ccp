package market

import java.io.File

import akka.actor.{Actor, Props}
import akka.pattern.pipe
import breeze.linalg.{Axis, DenseVector}
import breeze.stats.distributions.{Gaussian, MultivariateGaussian}
import cats.data.OptionT
import cats.implicits._
import com.github.tototoshi.csv.CSVWriter
import com.typesafe.scalalogging.Logger
import market.Market.{Index, Margin, Price}
import spire.implicits._
import spire.math
import structure.Timed.{Time, res, _}

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
  */
case class Market(prices: Map[Security, BigDecimal],
                  indexes: Map[Security, Index],
                  retDistr: MultivariateGaussian,
                  scaling: Int)
    extends Actor {
  private val icdf = (p: Double) => BigDecimal(Gaussian(0, 1).icdf(p))

  private var generatedPrices = Map(FiniteDuration(0, res) -> prices)

  private val logger = Logger("Market")

  private val f = new File("out.csv")

  def receive: Receive = {
    case Price(i, t) => sender ! price(i, t)
    case Margin(portfolio, coverage, timeHorizon, t) =>
      margin(portfolio, coverage, timeHorizon, t).value pipeTo sender
  }

  /**
    * Provide the price for the instrument at the given time.
    * @param time time for price.
    * @param instrument instrument
    * @return the price for the instrument at the given time.
    */
  private def price(instrument: Security, time: Time): Option[BigDecimal] = {

    /**
      * Helper function for generating prices for a specific point in time.
      * Will also generate prices for the missing past.
      * @param t point in time of prices.
      * @return prices for point in time t.
      */
    def generatePrices(t: Time): Option[Map[Security, BigDecimal]] = {
      val rs = retDistr.draw().map(BigDecimal(_))

      val newPsO = indexes.keys
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
            generatedPrices += (t - tick) -> ps

            // Price cannot be negative
            a -> ((p * (1 + (r / scaling))) max 0)
        })
        .toList
        .sequence[Option, (Security, BigDecimal)]
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
  private def margin(portfolio: Portfolio,
                     coverage: BigDecimal,
                     timeHorizon: Time,
                     time: Time): OptionT[Future, BigDecimal] = {
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
        price <- portfolio.price(time)

        pWeights <- OptionT.liftF(portfolio.weights(time).map(_.values))

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
  type Index = Int

  case class Price(instrument: Security, t: Time)
  case class Margin(portfolio: Portfolio, coverage: BigDecimal, timeHorizon: Time, t: Time)

  def props(prices: Map[Security, BigDecimal],
            indexes: Map[Security, Index],
            retDistr: MultivariateGaussian,
            scaling: Int): Props = Props(Market(prices, indexes, retDistr, scaling))
}
