package util

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import market.Market.Index
import market.Security
import structure.Timed
import structure.Timed.Time

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

  def readCsv(f: String,
              indexes: Map[Security, Index],
              prices: Map[Security, BigDecimal])
    : Map[Time, Map[Security, BigDecimal]] = {
    val reader = CSVReader.open(s"$f.csv")
    val data = reader.all()
    val flippedIndexes = indexes.map(_.swap)

    val rs = data
      .map(_.splitAt(1))
      .map {
        case (x, xs) =>
          (FiniteDuration(x.head.toDouble.toLong, Timed.res),
           xs.map(BigDecimal(_)).zipWithIndex.map(_.swap))
      }
      .map {
        case (x, xs) =>
          (x, xs.map {
            case (i, b) =>
              (flippedIndexes.getOrElse(i, throw new IllegalStateException()),
               b)
          })
      }
      .toMap
      .mapValues(_.toMap)

    val ps = rs.mapValues(_.map {
      case (s, r) =>
        s -> r * prices.getOrElse(s, throw new IllegalStateException())
    })

    ps + (Timed.zero -> prices)
  }

  def writeToCsv(filename: String)(
      data: Map[String, Map[Int, BigDecimal]]): Unit = {
    val w = CSVWriter open s"$filename.csv"

    for {
      (name, simulations) <- data
      orderedMovements = simulations.toList.sortBy(_._1).map(_._2)
    } yield w writeRow name :: orderedMovements

    w close ()
  }
}
