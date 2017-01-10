import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import breeze.linalg.DenseVector
import breeze.stats.distributions.MultivariateGaussian
import com.typesafe.config.ConfigFactory
import market.{Market, Portfolio, Security}
import structure._
import structure.ccp.Ccp._
import structure.ccp.Waterfall.{End, Start, _}
import structure.ccp.{Ccp, Waterfall}
import util.DataUtil.{generatePortfolios2, readCovMat, readFuturePrices, readPrices}

import scala.collection.immutable._
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Created by dennis on 8/10/16.
  */
object Main extends App {
  implicit val timeout: Timeout = Timeout(60 seconds)

  val system = ActorSystem("System", ConfigFactory.load())

  val pos0 = Security("pos0")
  val pos1 = Security("pos1")
  val sp500 = Security("sp500")

  var positions = Set(pos0, pos1, sp500)

  val securities = positions.zipWithIndex.map(_.swap).toMap
  val indexes = securities.map(_.swap)

  val psFile = "ps0"
  val fPsFile = "data0"
  val covFile = "cov0"

  val prices = readPrices(psFile)(securities)
  println(prices)
  val futurePrices = readFuturePrices(fPsFile)(securities)

  val scaling = BigDecimal(100000)

  val market = Market(
    prices = prices,
    indexes = indexes,
    retDistr = MultivariateGaussian(
      DenseVector(0.0, 0.0, 0.0),
      readCovMat(covFile)(scaling)
    ),
    scaling = scaling,
    futurePrices
  )

  val capitalInf = Portfolio(Map(sp500 -> BigDecimal("1E99")), market)
  val noCapital = Portfolio(Map(sp500 -> 0), market)

  val scheduler = system.actorOf(Scheduler.props(50 milliseconds))

//  val member0 = system.actorOf(
//    Member.props("member 0", capital = noCapital, scheduler = scheduler))

  val member1 =
    system.actorOf(
      Member.props(
        name = "member 1",
        capital = noCapital,
        scheduler = scheduler,
        shouldDefault = true
      )
    )

  val member2 =
    system.actorOf(
      Member.props(
        name = "member 2",
        capital = noCapital,
        scheduler = scheduler,
        shouldDefault = true
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
        capital = capitalInf,
        scheduler = scheduler
      )
    )

  val member5 =
    system.actorOf(
      Member.props(
        name = "member 5",
        capital = capitalInf,
        scheduler = scheduler
      )
    )

  val member6 =
    system.actorOf(
      Member.props(
        name = "member 6",
        capital = noCapital,
        scheduler = scheduler,
        shouldDefault = true
      )
    )

  val member7 =
    system.actorOf(
      Member.props(
        name = "member 7",
        capital = noCapital,
        scheduler = scheduler,
        shouldDefault = true
      )
    )

  val member8 =
    system.actorOf(
      Member.props(
        name = "member 8",
        capital = capitalInf,
        scheduler = scheduler
      )
    )

  val member9 =
    system.actorOf(
      Member.props(
        name = "member 9",
        capital = capitalInf,
        scheduler = scheduler
      )
    )

  val member10 =
    system.actorOf(
      Member.props(
        name = "member 10",
        capital = capitalInf,
        scheduler = scheduler
      )
    )

  val member11 =
    system.actorOf(
      Member.props(
        name = "member 11",
        capital = noCapital,
        scheduler = scheduler,
        shouldDefault = true
      )
    )

  val member12 =
    system.actorOf(
      Member.props(
        name = "member 12",
        capital = noCapital,
        scheduler = scheduler,
        shouldDefault = true
      )
    )

  val member13 =
    system.actorOf(
      Member.props(
        name = "member 13",
        capital = capitalInf,
        scheduler = scheduler
      )
    )

  val member14 =
    system.actorOf(
      Member.props(
        name = "member 14",
        capital = capitalInf,
        scheduler = scheduler
      )
    )

  val member15 =
    system.actorOf(
      Member.props(
        name = "member 15",
        capital = capitalInf,
        scheduler = scheduler
      )
    )

  val member16 =
    system.actorOf(
      Member.props(
        name = "member 16",
        capital = noCapital,
        scheduler = scheduler,
        shouldDefault = true
      )
    )

  val member17 =
    system.actorOf(
      Member.props(
        name = "member 17",
        capital = noCapital,
        scheduler = scheduler,
        shouldDefault = true
      )
    )

  val member18 =
    system.actorOf(
      Member.props(
        name = "member 18",
        capital = capitalInf,
        scheduler = scheduler
      )
    )

  val member19 =
    system.actorOf(
      Member.props(
        name = "member 19",
        capital = capitalInf,
        scheduler = scheduler
      )
    )

  val member20 =
    system.actorOf(
      Member.props(
        name = "member 6",
        capital = capitalInf,
        scheduler = scheduler
      )
    )

  val members = Set(
    member1,
    member2,
    member3,
    member4,
    member5,
    member6,
    member7,
    member8,
    member9,
    member10,
    member11,
    member12,
    member13,
    member14,
    member15,
    member16,
    member17,
    member18,
    member19,
    member20
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

  val ccp1: ActorRef = system.actorOf(
    Ccp.props[Security](
      name = "ccp1",
      waterfall = isdaWaterfall,
//      memberPortfolios = Map(
//        member1 -> member1Portfolio,
//        member3 -> member2Portfolio,
//        member4 -> member3Portfolio
//      ),
//      ccpPortfolios = Map(ccp2 -> ccp2Portfolio),
      capital = capitalInf,
      rules = Rules(
        maxCallPeriod = 180 minutes,
        maxRecapPeriod = 2000 minutes,
        minimumTransfer = BigDecimal(0),
        marginIsRingFenced = false,
        maximumFundCall = BigDecimal("1E99"),
        skinInTheGame = BigDecimal("0.1"),
        marginCoverage = BigDecimal("0.99"),
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

  val ccp2: ActorRef = system.actorOf(
    Ccp.props[Security](
      name = "ccp2",
      waterfall = isdaWaterfall,
//      memberPortfolios = Map(member4 -> member4Portfolio,
//                             member5 -> member5Portfolio,
//                             member6 -> member6Portfolio),
//      ccpPortfolios = Map(ccp1 -> ccp1Portfolio),
      capital = capitalInf,
      rules = Rules(
        maxCallPeriod = 180 minutes,
        maxRecapPeriod = 2000 minutes,
        minimumTransfer = BigDecimal(0),
        marginIsRingFenced = false,
        maximumFundCall = BigDecimal("1E99"),
        skinInTheGame = BigDecimal("0.1"),
        marginCoverage = BigDecimal("0.99"),
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

  val ccpMembers = Map(
    ccp1 -> Set(member1,
                member2,
                member3,
                member4,
                member5,
                member6,
                member7,
                member8,
                member9,
                member10),
    ccp2 -> Set(member11,
                member12,
                member13,
                member14,
                member15,
                member16,
                member17,
                member18,
                member19,
                member20)
  )

  val actorIndexes = Map(
    member1 -> 1,
    member2 -> 2,
    member3 -> 3,
    member4 -> 4,
    member5 -> 5,
    member6 -> 6,
    member7 -> 7,
    member8 -> 8,
    member9 -> 9,
    member10 -> 10,
    member11 -> 11,
    member12 -> 12,
    member13 -> 13,
    member14 -> 14,
    member15 -> 15,
    member16 -> 16,
    member17 -> 17,
    member18 -> 18,
    member19 -> 19,
    member20 -> 20,
    ccp1 -> 21,
    ccp2 -> 22
  )

  val sizes: Map[ActorRef, BigDecimal] = Map(
    member1 -> 100,
    member2 -> 100,
    member3 -> 100,
    member4 -> 100,
    member5 -> 100,
    member6 -> 100,
    member7 -> 100,
    member8 -> 100,
    member9 -> 100,
    member10 -> 100,
    member11 -> 100,
    member12 -> 100,
    member13 -> 100,
    member14 -> 100,
    member15 -> 100,
    member16 -> 100,
    member17 -> 100,
    member18 -> 100,
    member19 -> 100,
    member20 -> 100
  )

  val memberPositions = Map(
    member1 -> pos0,
    member2 -> pos0,
    member3 -> pos0,
    member4 -> pos0,
    member5 -> pos0,
    member6 -> pos1,
    member7 -> pos1,
    member8 -> pos1,
    member9 -> pos1,
    member10 -> pos1,
    member11 -> pos0,
    member12 -> pos0,
    member13 -> pos0,
    member14 -> pos0,
    member15 -> pos0,
    member16 -> pos1,
    member17 -> pos1,
    member18 -> pos1,
    member19 -> pos1,
    member20 -> pos1
  )

  val genPortfolios = generatePortfolios2(
    sizes = sizes,
    positions = memberPositions,
    ccpInstruments = Map(ccp1 -> Set(pos0), ccp2 -> Set(pos1)),
    ccpMembers = ccpMembers,
    market = market
  )

//  val foo = (for {
//    (k, v) <- genPortfolios
//    newK = actorIndexes.getOrElse(k, throw new IllegalArgumentException())
//    newV = v.positions
//  } yield newK -> newV).toMap
//
//  util.DataUtil.writePortfoliosToCsv("p1")(foo)

  println(s"ccp1 as $ccp1")
  println(s"ccp2 as $ccp2")
  println(s"scheduler as $scheduler")
  println(s"member1 as $member1")
  println(s"member2 as $member2")
  println(s"member3 as $member3")
  println(s"member4 as $member4")
  println(s"member5 as $member5")
  println(s"member6 as $member6")

  val scenario = system.actorOf(
    Scenario.props(
      ccps = Set(ccp1, ccp2),
      members = members,
      ccpMembers = ccpMembers,
      ccpLinks = Map(ccp1 -> Set(ccp2), ccp2 -> Set(ccp1)),
      timeHorizon = 1950 minutes,
      callEvery = 15 minutes,
      runs = 2,
      portfolios = genPortfolios,
      scheduler = scheduler
    )
  )

  println(s"scenario as $scenario")

  scenario.tell(Scenario.Run, null)
}
