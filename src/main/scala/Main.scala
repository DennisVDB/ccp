import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.MultivariateGaussian
import com.typesafe.config.ConfigFactory
import market.{Market, Portfolio, Security}
import structure._
import structure.ccp.Ccp._
import structure.ccp.Waterfall.{End, Start, _}
import structure.ccp.{Ccp, Waterfall}
import util.DataUtil.readCsv

import scala.collection.immutable._
import scala.concurrent.duration._
import scala.language.postfixOps
import scalaz.Scalaz._
import scalaz._

/**
  * Created by dennis on 8/10/16.
  */
object Main extends App {
  implicit val timeout: Timeout = Timeout(60 seconds)

  val system = ActorSystem("System", ConfigFactory.load())

  val pos1 = Security("pos1")
  val pos2 = Security("pos2")
  val pos3 = Security("pos3")
  val pos4 = Security("pos4")
  val pos5 = Security("pos5")
  val pos6 = Security("pos6")
  val sp500 = Security("sp500")

//  val market = system.actorOf(
//    Market.props(
//      prices = Map(pos1 -> BigDecimal("10000"), pos2 -> BigDecimal("5000")),
//      indexes = Map(pos1 -> 0, pos2 -> 1),
//      retDistr = MultivariateGaussian(
//        DenseVector(0.0, 0.0), //DenseVector(0.0002, 0.00015),
//        DenseMatrix((1.0, 0.0), (0.0, 0.2))
//      ),
//      scaling = 100
//    ))

  val indexes = Map(pos1 -> 0,
                    pos2 -> 1,
                    pos3 -> 2,
                    pos4 -> 3,
                    pos5 -> 4,
                    pos6 -> 5,
                    sp500 -> 6)

  val prices = Map(
    pos1 -> BigDecimal("10000"),
    pos2 -> BigDecimal("5000"),
    pos3 -> BigDecimal("3000"),
    pos4 -> BigDecimal("1500"),
    pos5 -> BigDecimal("1000"),
    pos6 -> BigDecimal("1800"),
    sp500 -> BigDecimal("100")
  )

  val market = Market(
    prices = prices,
    indexes = indexes,
    retDistr = MultivariateGaussian(
      DenseVector(-0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
      DenseMatrix(
        (0.01, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        (0.0, 0.01, 0.0, 0.0, 0.0, 0.0, 0.0),
        (0.0, 0.0, 0.01, 0.0, 0.0, 0.0, 0.0),
        (0.0, 0.0, 0.0, 0.01, 0.0, 0.0, 0.0),
        (0.0, 0.0, 0.0, 0.0, 0.1, 0.0, 0.0),
        (0.0, 0.0, 0.0, 0.0, 0.0, 0.1, 0.0),
        (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.1)
      )
    ),
    scaling = 1000000,
    readCsv("out", indexes, prices)
  )

  // Some(readCsv("out", Map(pos1 -> 0, pos2 -> 1, pos3 -> 2, sp500 -> 3)))

  val longPos1 = Portfolio(Map(pos1 -> 100), market)
  val longPos2 = Portfolio(Map(pos2 -> 100), market)
  val longPos3 = Portfolio(Map(pos3 -> 100), market)
  val longPos4 = Portfolio(Map(pos4 -> 100), market)
  val longPos5 = Portfolio(Map(pos5 -> 100), market)
  val longPos6 = Portfolio(Map(pos6 -> 100), market)

  val shortPos1 = Portfolio(Map(pos1 -> -20), market)
  val shortPos2 = Portfolio(Map(pos2 -> -20), market)
  val shortPos3 = Portfolio(Map(pos3 -> -20), market)
  val shortPos4 = Portfolio(Map(pos4 -> -20), market)
  val shortPos5 = Portfolio(Map(pos5 -> -20), market)
  val shortPos6 = Portfolio(Map(pos6 -> -20), market)

  val member1Portfolio = longPos1 |+| shortPos2 |+| shortPos3 |+| shortPos4 |+| shortPos5 |+| shortPos6
  val member2Portfolio = longPos2 |+| shortPos1 |+| shortPos3 |+| shortPos4 |+| shortPos5 |+| shortPos6
  val member3Portfolio = longPos3 |+| shortPos1 |+| shortPos2 |+| shortPos4 |+| shortPos5 |+| shortPos6
  val member4Portfolio = longPos4 |+| shortPos1 |+| shortPos2 |+| shortPos3 |+| shortPos5 |+| shortPos6
  val member5Portfolio = longPos5 |+| shortPos1 |+| shortPos2 |+| shortPos3 |+| shortPos4 |+| shortPos6
  val member6Portfolio = longPos6 |+| shortPos1 |+| shortPos2 |+| shortPos3 |+| shortPos4 |+| shortPos5

  val ccp1Portfolio = (
    shortPos4 |+| shortPos5 |+| shortPos6 |+|
      shortPos4 |+| shortPos5 |+| shortPos6 |+|
      shortPos4 |+| shortPos5 |+| shortPos6
  ).inverse

  val ccp2Portfolio = {
    shortPos1 |+| shortPos2 |+| shortPos3 |+|
      shortPos1 |+| shortPos2 |+| shortPos3 |+|
      shortPos1 |+| shortPos2 |+| shortPos3
  }.inverse

  val emptyPortfolio = Portfolio(Map(pos3 -> 0), market)
  val capital = Portfolio(Map(sp500 -> 1), market)
  val capitalInf = Portfolio(Map(sp500 -> 100000), market)

  val scheduler = system.actorOf(Scheduler.props(50 milliseconds))

  val member1 =
    system.actorOf(
      Member.props(
        name = "member 1",
        capital = capital,
        scheduler = scheduler
      )
    )

  val member2 =
    system.actorOf(
      Member.props(
        name = "member 2",
        capital = capital,
        scheduler = scheduler
      )
    )

  val member3 =
    system.actorOf(
      Member.props(
        name = "member 3",
        capital = capitalInf,
        scheduler = scheduler
      )
    )

  val member4 =
    system.actorOf(
      Member.props(
        name = "member 4",
        capital = emptyPortfolio,
        scheduler = scheduler
      )
    )

  val member5 =
    system.actorOf(
      Member.props(
        name = "member 5",
        capital = emptyPortfolio,
        scheduler = scheduler
      )
    )

  val member6 =
    system.actorOf(
      Member.props(
        name = "member 6",
        capital = emptyPortfolio,
        scheduler = scheduler
      )
    )

  val arnsdorfWaterfall = Waterfall(
    Map(Start -> Defaulted,
        Defaulted -> FirstLevelEquity,
        FirstLevelEquity -> Survivors,
        Survivors -> Unfunded,
        Unfunded -> SecondLevelEquity,
        SecondLevelEquity -> End))

  val isdaWaterfall = Waterfall(
    Map(Start -> Defaulted,
        Defaulted -> FirstLevelEquity,
        FirstLevelEquity -> Survivors,
        Survivors -> SecondLevelEquity,
        SecondLevelEquity -> VMGH,
        VMGH -> End))

  lazy val ccp1: ActorRef = system.actorOf(
    Ccp.props[Security](
      name = "ccp1",
      waterfall = isdaWaterfall,
      memberPortfolios = Map(
        member1 -> member1Portfolio,
        member3 -> member2Portfolio,
        member4 -> member3Portfolio
      ),
      ccpPortfolios = Map(ccp2 -> ccp2Portfolio),
      capital = capital,
      rules = Rules(
        maxCallPeriod = 480 minutes,
        maxRecapPeriod = 7200 minutes,
        minimumTransfer = BigDecimal(0),
        marginIsRingFenced = false,
        maximumFundCall = BigDecimal(1000),
        skinInTheGame = BigDecimal("0.1"),
        marginCoverage = BigDecimal("0.95"),
        timeHorizon = 1440 minutes,
        fundParticipation = BigDecimal("0.2"),
        ccpRules = CcpRules(
          participatesInMargin = true
        )
      ),
      operationalDelays = OperationalDelays(
        callHandling = 0 minutes,
        coverWithMargin = 0 minutes,
        coverWithFund = 0 minutes,
        coverWithSurvivorsMargins = 0 minutes,
        coverWithSurvivorsFunds = 0 minutes,
        computeSurvivorsUnfunded = 0 minutes,
        coverWithSurvivorsUnfunded = 0 minutes,
        coverWithVMGH = 0 minutes,
        coverWithFirstLevelEquity = 0 minutes,
        coverWithSecondLevelEquity = 0 minutes
      ),
      scheduler = scheduler
    )
  )

  lazy val ccp2: ActorRef = system.actorOf(
    Ccp.props[Security](
      name = "ccp2",
      waterfall = arnsdorfWaterfall,
      memberPortfolios = Map(member4 -> member4Portfolio,
                             member5 -> member5Portfolio,
                             member6 -> member6Portfolio),
      ccpPortfolios = Map(ccp1 -> ccp1Portfolio),
      capital = capital,
      rules = Rules(
        maxCallPeriod = 480 minutes,
        maxRecapPeriod = 7200 minutes,
        minimumTransfer = BigDecimal(0),
        marginIsRingFenced = false,
        maximumFundCall = BigDecimal(1000),
        skinInTheGame = BigDecimal("0.1"),
        marginCoverage = BigDecimal("0.95"),
        timeHorizon = 1440 minutes,
        fundParticipation = BigDecimal("0.2"),
        ccpRules = CcpRules(
          participatesInMargin = true
        )
      ),
      operationalDelays = OperationalDelays(
        callHandling = 0 minutes,
        coverWithMargin = 0 minutes,
        coverWithFund = 0 minutes,
        coverWithSurvivorsMargins = 0 minutes,
        coverWithSurvivorsFunds = 0 minutes,
        computeSurvivorsUnfunded = 0 minutes,
        coverWithSurvivorsUnfunded = 0 minutes,
        coverWithVMGH = 0 minutes,
        coverWithFirstLevelEquity = 0 minutes,
        coverWithSecondLevelEquity = 0 minutes
      ),
      scheduler = scheduler
    )
  )

  println(s"ccp1 as $ccp1")
//  println(s"ccp2 as $ccp2")
  println(s"member1 as $member1")
  println(s"member2 as $member2")
  println(s"member3 as $member3")
  println(s"member4 as $member4")

  val scenario = system.actorOf(
    Scenario.props(ccps = Set(ccp1, ccp2),
                   members = Set(member1, member2, member3, member4, member5, member6),
                   timeHorizon = 2 day,
                   callEvery = 15 minutes,
      10,
                   scheduler = scheduler)
  )

  Thread.sleep(2000)

  scenario.tell(Scenario.Run, null)
}
