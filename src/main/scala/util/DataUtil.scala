package util

import com.github.tototoshi.csv.CSVReader
import market.Market.Index
import market.Security
import structure.Timed
import structure.Timed.Time

import scala.collection.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
  * Created by dennis on 12/10/16.
  */
object DataUtil {
  def mapFToFMap[A, B](m: Map[A, Future[B]]): Future[Map[A, B]] = {
    Future
      .traverse(m) {
        case (a, fb) => fb.map((a, _))
      }
      .map(xs => Map(xs.toSeq: _*))
  }

  def optionFutureToFutureOption[A](of: Option[Future[A]]): Future[Option[A]] = {
    of.map(f => f.map(Option(_))).getOrElse(Future.successful(None))
  }

  def sumList[A](l: List[Option[A]])(implicit ev: Numeric[A]): Option[A] = {
    l.foldLeft(Option(ev.zero)) {
      case (acc, el) =>
        el.flatMap(value => acc.map(ac => ev.plus(ac, value)))
    }
  }

  def readCsv(f: String, indexes: Map[Security, Index]): Map[Time, Map[Security, BigDecimal]] = {
    val reader = CSVReader.open(s"$f.csv")
    val data = reader.all()
    val flippedIndexes = indexes.map(_.swap)

    val foo = data
      .map(_.splitAt(1))
      .map {
        case (x, xs) =>
          (FiniteDuration(x.head.toDouble.toLong, Timed.res),
           xs.map(BigDecimal(_)).zipWithIndex.map(_.swap))
      }
      .map {
        case (x, xs) =>
          (x, xs.map {
            case (i, b) => (flippedIndexes.getOrElse(i, throw new IllegalStateException()), b)
          })
      }
      .toMap
      .mapValues(_.toMap)

    print(foo)

    foo
  }

//  def bigDecimalMonoid = new Monoid[BigDecimal] {
//    def zero: BigDecimal = BigDecimal(0)
//
//    def append(f1: BigDecimal, f2: => BigDecimal): BigDecimal = f1 + f2
//  }

//  // From https://gist.github.com/flightonary/404a94791594d7f568f1, changed from Java -> Scala
//  def sqrt(a: BigDecimal, scale: Int): BigDecimal = {
//    var x = BigDecimal(Math.sqrt(a.doubleValue()))
//
//    if (scale < 17) {
//      return x
//    }
//
//    var tempScale = 16
//    while (tempScale < scale) {
//      //x = x - (x * x - a) / (2 * x);
//
//      x = x - (x * x - a) / (2 * x)
//      tempScale *= 2
//    }
//
//    x
//  }
}
