import Market.{Index, Price}
import breeze.linalg.{Axis, DenseVector}
import breeze.numerics.sqrt
import breeze.stats.distributions.{Gaussian, MultivariateGaussian}

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

        weights = portfolio.weights.values

        pWeights = DenseVector(weights.toList: _*)
          .map(_.doubleValue)
          .asDenseMatrix

        mus = distr.mean.toDenseMatrix
          .delete(indexesToStrip.toSeq, Axis._1)
          .toDenseVector

        pCov = distr.covariance
          .delete(indexesToStrip.toSeq, Axis._0)
          .delete(indexesToStrip.toSeq, Axis._1)

        varM = pWeights.t * pCov * pWeights
        if varM.rows == 1 && varM.cols == 1

        pSigma = sqrt(varM(0, 0))

        z = icdf(coverage.doubleValue)

        pMu = (mus :* pWeights.toDenseVector).toArray.sum

        valueAtRisk = price * (pMu - pSigma * z)
      } yield -valueAtRisk // valueAtRisk is negative, we flip the sign.
    }
  }
}

object Market {
  type Price = BigDecimal
  type Index = Int
}
