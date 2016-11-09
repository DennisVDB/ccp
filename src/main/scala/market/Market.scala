import breeze.stats.distributions.Gaussian

import scala.collection.concurrent.Map

/**
  * Created by dennis on 9/10/16.
  */
case class Market[A](items: Map[A, PriceData]) {
  def price(item: A): Option[BigDecimal] = {
    for {
      priceData <- items.get(item)
    } yield priceData.price
  }

  def shockAll(shock: BigDecimal): Unit = {
    items.keys.foreach(shockItem(_, shock))
  }

  def shockItem(item: A, shock: BigDecimal) = {
    for {
      priceData <- items.get(item)
      shockedPriceData = PriceData(priceData.price * (1 + shock),
                                   priceData.returnDistr)
    } items.replace(item, shockedPriceData)
  }

  def returnDistr(item: A): Option[Gaussian] = {
    for {
      priceData <- items.get(item)
    } yield priceData.returnDistr
  }
}

case class PriceData(price: BigDecimal, returnDistr: Gaussian)
