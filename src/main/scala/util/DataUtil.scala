package util

import akka.actor.ActorRef
import breeze.linalg.DenseMatrix
import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import com.typesafe.scalalogging.Logger
import market.Market.Index
import market.{Market, Portfolio, Security}
import structure.Timed
import structure.Timed.Time

import scala.annotation.tailrec
import scala.collection.immutable.{List, Map}
import scala.concurrent.duration.FiniteDuration
import scalaz.Scalaz._
import scalaz._

/**
  * Created by dennis on 12/10/16.
  */
object DataUtil {

  val logger = Logger("UTIL")

//  /**
//    * Generates the portfolios for the members and the inter-CCP links.
//    * @param testMember Member that will be used to hide liquidity.
//    * @param testMemberSplitOver Over how many CCPs the test member has split his portfolio.
//    * @param memberSizes Size of the portfolio long positions at each CCP.
//    *                    For the test member, the size of the portfolio is the size of the portfolio in each CCP.
//    *                    Hence, it has an equal portfolio in each one of then.
//    * @param ps Securities to put in the portfolios.
//    * @param ccpMembers List of members of each CCP.
//    * @param market The market on which the securities are traded.
//    * @return A generated portfolio for each member and CCP.
//    */
//  def generatePortfolios1(testMember: ActorRef,
//                          testMemberSplitOver: Int,
//                          memberSizes: Map[ActorRef, BigDecimal],
//                          ps: List[Security],
//                          ccpMembers: Map[ActorRef, Set[ActorRef]],
//                          market: Market): Map[ActorRef, Portfolio] = {
//
//    def genPositions(ps: List[Security],
//                     amount: BigDecimal): Map[Security, BigDecimal] = {
//      val rawAmounts = ps.map((_, BigDecimal(Random.nextDouble()))).toMap
//
//      // Normalize values
//      val sum = rawAmounts.values.sum
//      rawAmounts.mapValues { x =>
//        amount * (x / sum)
//      }
//    }
//
//    val memberLongSubPortfolios = (for {
//      (m, size) <- memberSizes
//
//      longPortfolio = if (m == testMember)
//        Portfolio(genPositions(ps, size),
//                  market,
//                  splitOver = testMemberSplitOver)
//      else
//        Portfolio(genPositions(ps, size), market)
//
//      counterparties = memberSizes.keys.filter(_ != m)
//
//      // Split long portfolio into sub-portfolios
//      // each with a counterparty.
//      longSubPortfolios = counterparties.map { c =>
//        c -> longPortfolio / counterparties.size
//      }.toMap
//    } yield m -> longSubPortfolios).toMap
//
//    val memberLongPortfolios = (for {
//      (m, longSubs) <- memberLongSubPortfolios
//      head = longSubs.values.head
//      tail = longSubs.values.tail.toSeq
//      longPortfolio = NonEmptyList(head, tail: _*).sumr1
//    } yield m -> longPortfolio).toMap
//
//    val memberShortSubPortfolios = (for {
//      m <- memberSizes.keys
//
//      // Long portfolios of the counterparties.
//      otherLongs = memberLongPortfolios - m
//
//      // Take short version of long portfolios,
//      // and split them with all the counterparties,
//      // not including the test member. The test member
//      // has no short positions.
//      shortSubs = otherLongs.mapValues { p =>
//        p.inverse / otherLongs.size
//      }
//    } yield m -> shortSubs).toMap
//
//    val memberSubPortfolios = memberLongSubPortfolios |+| memberShortSubPortfolios
//
//    val memberPortfolios = (for {
//      (m, subs) <- memberSubPortfolios
//      head = subs.values.head
//      tail = subs.values.tail.toSeq
//      portfolio = NonEmptyList(head, tail: _*).sumr1
//    } yield m -> portfolio).toMap
//
//    val ccpSubPortfolios = for {
//      (ccp, members) <- ccpMembers
//
//      // Sub-portfolios of the ccp members.
//      ccpMemberSubPortfolios = memberSubPortfolios.filterKeys(members.contains)
//
//      // Linked CCPs and its members.
//      (linkedCcp, linkedMembers) <- ccpMembers.filterKeys(_ != ccp)
//
//      // Sub-portfolios having a counterparty in the linked CCP.
//      subPortfolios <- ccpMemberSubPortfolios
//        .filterKeys(linkedMembers.contains)
//        .values
//    } yield linkedCcp -> subPortfolios
//
//    val ccpPortfolios = (for {
//      (ccp, subs) <- ccpSubPortfolios
//      head = subs.values.head
//      tail = subs.values.tail.toSeq
//      portfolio = NonEmptyList(head, tail: _*).sumr1
//    } yield ccp -> portfolio).toMap
//
//    memberPortfolios |+| ccpPortfolios
//  }

  def generatePortfolios(sizes: Map[ActorRef, BigDecimal],
                         positions: Map[ActorRef, Security],
                         illiquidityMember: ActorRef,
                         illiquidityMult: Int,
                         ccpInstruments: Map[ActorRef, Set[Security]],
                         ccpMembers: Map[ActorRef, Set[ActorRef]],
                         market: Market): Portfolios = {
    val memberLongSubs = (for {
      (m, size) <- sizes
      p = positions.getOrElse(m, throw new IllegalStateException())
      counterparties = sizes.keySet - m

      longPortfolio = if (m == illiquidityMember)
        Portfolio(Map(p -> size), market, splitOver = illiquidityMult)
      else
        Portfolio(Map(p -> size), market)

      longSubs = counterparties.map { c =>
        c -> longPortfolio / counterparties.size
      }.toMap

    } yield m -> longSubs).toMap

    val memberLongs = (for {
      (m, longSubs) <- memberLongSubs
      head = longSubs.values.head
      tail = longSubs.values.tail.toSeq
      longPortfolio = NonEmptyList(head, tail: _*).sumr1
    } yield m -> longPortfolio).toMap

    val memberShortSubs = (for {
      m <- sizes.keys

      // Long portfolios of the counterparties.
      otherLongs = memberLongs - m

      // Take short version of long portfolios,
      // and split them with all the counterparties,
      // not including the test member. The test member
      // has no short positions.
      shortSubs = otherLongs.mapValues { p =>
        p.inverse / otherLongs.size
      }
    } yield m -> shortSubs).toMap

    val memberSubs = memberLongSubs |+| memberShortSubs

    val memberPortfolios = (for {
      (m, subs) <- memberSubs
      head = subs.values.head
      tail = subs.values.tail.toSeq
      portfolio = NonEmptyList(head, tail: _*).sumr1
    } yield m -> portfolio).toMap

    val ccpPortfolios = (for {
      (ccp, members) <- ccpMembers
      instruments = ccpInstruments.getOrElse(ccp,
                                             throw new IllegalStateException())

      linkedPortfolios = for {
        (linkedCcp, linkedMembers) <- ccpMembers - ccp
        linkedSubs <- memberSubs.filterKeys(linkedMembers.contains).values
        exposedSubs = linkedSubs.values
          .map(_.positions)
          .map(_.filterKeys(instruments.contains))
        head = exposedSubs.head
        tail = exposedSubs.tail.toSeq
        positions = NonEmptyList(head, tail: _*).sumr1
        portfolio = Portfolio(positions, market)
      } yield linkedCcp -> portfolio
    } yield ccp -> linkedPortfolios).toMap

    Portfolios(memberPortfolios, ccpPortfolios)
  }

  def generatePortfolios2(sizes: Map[ActorRef, BigDecimal],
                          positions: Map[ActorRef, Security],
                          ccpInstruments: Map[ActorRef, Set[Security]],
                          ccpMembers: Map[ActorRef, Set[ActorRef]],
                          market: Market): Portfolios = {
    val memberLongSubs = (for {
      (m, size) <- sizes
      p = positions.getOrElse(m, throw new IllegalStateException())
      counterparties = sizes.keySet - m

      longPortfolio = Portfolio(Map(p -> size), market)

      longSubs = counterparties.map { c =>
        c -> longPortfolio / counterparties.size
      }.toMap

    } yield m -> longSubs).toMap

    val memberLongs = (for {
      (m, longSubs) <- memberLongSubs
      head = longSubs.values.head
      tail = longSubs.values.tail.toSeq
      longPortfolio = NonEmptyList(head, tail: _*).sumr1
    } yield m -> longPortfolio).toMap

    val memberShortSubs = (for {
      m <- sizes.keys

      // Long portfolios of the counterparties.
      otherLongs = memberLongs - m

      // Take short version of long portfolios,
      // and split them with all the counterparties,
      // not including the test member. The test member
      // has no short positions.
      shortSubs = otherLongs.mapValues { p =>
        p.inverse / otherLongs.size
      }
    } yield m -> shortSubs).toMap

    val memberSubs = memberLongSubs |+| memberShortSubs

    val memberPortfolios = (for {
      (m, subs) <- memberSubs
      head = subs.values.head
      tail = subs.values.tail.toSeq
      portfolio = NonEmptyList(head, tail: _*).sumr1
    } yield m -> portfolio).toMap

    val ccpPortfolios = (for {
      (ccp, members) <- ccpMembers
      instruments = ccpInstruments.getOrElse(ccp,
                                             throw new IllegalStateException())

      linkedPortfolios = (for {
        (linkedCcp, linkedMembers) <- ccpMembers - ccp

        linkedPositions = memberPortfolios
          .filterKeys(linkedMembers.contains)
          .values
          .map(_.positions.filterKeys(instruments.contains))

        head = linkedPositions.head
        tail = linkedPositions.tail.toSeq
        positions = NonEmptyList(head, tail: _*).sumr1

      } yield linkedCcp -> Portfolio(positions, market)).toMap
    } yield ccp -> linkedPortfolios).toMap

    Portfolios(memberPortfolios, ccpPortfolios)
  }

  case class Portfolios(members: Map[ActorRef, Portfolio],
                        ccps: Map[ActorRef, Map[ActorRef, Portfolio]])

  def readFuturePrices(f: String)(
      is: Map[Index, Security]): Map[Time, Map[Security, BigDecimal]] = {
    val reader = CSVReader.open(s"$f.csv")
    val data = reader.all()

    data
      .map(_.splitAt(1))
      .map {
        case (x, xs) =>
          (FiniteDuration(x.head.toDouble.toLong - 1, Timed.res),
           xs.map(BigDecimal(_)).zipWithIndex.map(_.swap))
      }
      .map {
        case (x, xs) =>
          (x, xs.map {
            case (i, b) =>
              (is.getOrElse(i, throw new IllegalStateException()), b)
          })
      }
      .toMap
      .mapValues(_.toMap)
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

  def writePortfoliosToCsv(f: String)(
      ps: Map[Index, Map[Security, BigDecimal]]): Unit = {
    val w = CSVWriter.open(s"$f.csv")

    for {
      (i, p) <- ps
      (s, n) <- p
      write = w.writeRow(Seq(i.toString, s.name, n.toString))
    } yield write

    w.close()
  }

  def readPortfoliosFromCsv(f: String): Map[Index, Map[Security, BigDecimal]] = {
    val r = CSVReader.open(s"$f.csv")

    val lines = r.all()

    @tailrec
    def go(data: List[List[String]],
           acc: Map[Index, Map[Security, BigDecimal]])
      : Map[Index, Map[Security, BigDecimal]] = data match {
      case Nil => acc
      case x :: xs =>
        x match {
          case i :: name :: n :: Nil =>
            val line = Map(i.toInt -> Map(Security(name) -> BigDecimal(n)))
            go(xs, line |+| acc)
          case _ =>
            throw new IllegalArgumentException(s"Did not expect line: $x")
        }
    }

    go(lines, Map.empty)
  }

  def readCovMat(f: String)(scaling: BigDecimal): DenseMatrix[Double] = {
    val r = CSVReader.open(s"$f.csv")

    val ls = r.all

    val rows = ListT(ls).map(BigDecimal(_) * scaling).map(_.toDouble).run

    DenseMatrix(rows: _*)
  }

  def readPrices(f: String)(
      is: Map[Index, Security]): Map[Security, BigDecimal] = {
    def r = CSVReader.open(s"$f.csv")

    val lines = r.all()

    val pricesWithIndex = lines.map(_.head).zipWithIndex

    val securitiesWithPrice = for {
      (price, index) <- pricesWithIndex
      security = is.getOrElse(index, throw new IllegalArgumentException())
    } yield security -> BigDecimal(price)

    securitiesWithPrice.toMap
  }
}
