package market

import breeze.linalg.{Axis, DenseVector}
import breeze.numerics.sqrt
import breeze.stats.distributions.{Gaussian, MultivariateGaussian}
import cats.implicits._
import market.Market.{Index, Price}
import structure.Timed.Time

import scala.collection._

/**
  * Created by dennis on 9/10/16.
  */
case class Market[A](prices: Map[A, Price], indexes: Map[A, Index], distr: MultivariateGaussian) {
  private val icdf = (p: Double) => BigDecimal(Gaussian(0, 1).icdf(p))

  private val generatedPrices = concurrent.TrieMap(0 -> prices)

  def price(time: Time)(item: A): Option[Price] = {
    def generatePrices(t: Time): Option[Map[A, Price]] = {
      val rs = distr.draw()

      indexes.keys
        .map(a =>
          for {
            ps <- generatedPrices.get(t - 1).orElse(generatePrices(t - 1))
            p <- ps.get(a)
            i <- indexes.get(a)
            r = rs(i)
          } yield a -> p * (1 + r))
        .toList
        .sequence[Option, (A, Price)]
        .map(_.toMap)
    }

    generatedPrices.get(time) match {
      case Some(ps) => ps.get(item)
      case None =>
        for {
          ps <- generatePrices(time)
          p <- ps.get(item)
        } yield p
    }
  }

  def margin(time: Time)(portfolio: Portfolio[A], coverage: BigDecimal): Option[BigDecimal] = {
    val pItems = portfolio.positions.keys

    val pIndexes = indexes.filterKeys(pItems.toSet)

    val indexesToStrip = indexes.filterNot(x => pIndexes.contains(x._1)).values

    if (pIndexes.size != portfolio.positions.size)
      None // Market does not contain all portfolio elements.
    else {
      for {
        price <- portfolio.price(time)

        // We compute for a long portfolio
        p = if (price >= 0) portfolio else portfolio.inverse

        weights = p.weights(time).values // .map(w => if (price < 0) -w else w)

        pWeights = DenseVector(weights.toList: _*).map(_.doubleValue).asDenseMatrix

        muVec = distr.mean.toDenseMatrix.delete(indexesToStrip.toSeq, Axis._1).toDenseVector

        pCov = distr.covariance
          .delete(indexesToStrip.toSeq, Axis._0)
          .delete(indexesToStrip.toSeq, Axis._1)

        varM = pWeights.t * pCov * pWeights
        if varM.rows == 1 && varM.cols == 1

        pSigma = sqrt(varM(0, 0))

        z = icdf(coverage.doubleValue)

        pMu = (muVec :* pWeights.toDenseVector).toArray.sum

        // If base portfolio is long we take lower bound, if long we take the upper bound.
        ret = if (price >= 0) pMu - pSigma * z else pMu + pSigma * z

        valueAtRisk = price * ret
      } yield -valueAtRisk // valueAtRisk is negative, we flip the sign.
    }
  }
}

object Market {
  type Price = BigDecimal
  type Index = Int
}
