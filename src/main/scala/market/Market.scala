package market

import breeze.linalg.{Axis, DenseVector}
import breeze.numerics.sqrt
import breeze.stats.distributions.{Gaussian, MultivariateGaussian}
import market.Market.{Index, Price}

import scala.collection._

/**
  * Created by dennis on 9/10/16.
  */
case class Market[A](prices: concurrent.Map[A, Price],
                     indexes: Map[A, Index],
                     distr: MultivariateGaussian) {
  private val icdf = (p: Double) => BigDecimal(Gaussian(0, 1).icdf(p))

  def price(item: A): Option[Price] = {
    for {
      price <- prices.get(item)
    } yield price
  }

  def shockAll(shock: BigDecimal): Unit = {
    prices.keys.foreach(shockItem(_, shock))
  }

  def shockItem(item: A, shock: BigDecimal): Unit = {
    for {
      price <- prices.get(item)
      shockedPriceData = price * (1 + shock)
    } prices.replace(item, shockedPriceData)
  }

  def margin(portfolio: Portfolio[A],
             coverage: BigDecimal): Option[BigDecimal] = {
    val pItems = portfolio.positions.keys

    val pIndexes = indexes.filterKeys(pItems.toSet)

    val indexesToStrip = indexes.filterNot(x => pIndexes.contains(x._1)).values

    if (pIndexes.size != portfolio.positions.size)
      None // Market does not contain all portfolio elements.
    else {
      for {
        price <- portfolio.price

        // We compute for a long portfolio
        p = if (price >= 0) portfolio else portfolio.inverse

        weights = p.weights.values // .map(w => if (price < 0) -w else w)

        pWeights = DenseVector(weights.toList: _*)
          .map(_.doubleValue)
          .asDenseMatrix

        muVec = distr.mean.toDenseMatrix
          .delete(indexesToStrip.toSeq, Axis._1)
          .toDenseVector

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
