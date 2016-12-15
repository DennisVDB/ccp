import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.MultivariateGaussian
import com.typesafe.config.ConfigFactory
import market.{Market, Portfolio, Security}
import structure.Ccp._
import structure.Scenario.Run
import structure.Timed.Time
import structure._

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Created by dennis on 8/10/16.
  */
object Main extends App {

  implicit val timeout: Timeout = Timeout(60 seconds)

  val system = ActorSystem("System", ConfigFactory.load())

  val pos1 = Security("pos1")
  val pos2 = Security("pos2")

  val market = system.actorOf(
    Market.props(
      prices = Map(pos1 -> BigDecimal("10000"), pos2 -> BigDecimal("5000")),
      indexes = Map(pos1 -> 0, pos2 -> 1),
      retDistr = MultivariateGaussian(
        DenseVector(0.0, 0.0), //DenseVector(0.0002, 0.00015),
        DenseMatrix((0.5, 0.0), (0.0, 0.2))
      ),
      scaling = 100
    ))

  val longPortfolio: Portfolio =
    Portfolio(positions = Map((pos1, 1)), liquidity = 1 minute, market)

  val shortPortfolio = longPortfolio.inverse

  val emptyPortfolio =
    Portfolio(positions = Map.empty[Security, Int], liquidity = 0 minutes, market)

  implicit val scheduler = system.actorOf(Scheduler.props(1 milliseconds))

  val member1 =
    system.actorOf(
      Member.props(
        name = "member 1",
        assets = Map(
          (0 minutes) -> BigDecimal(100),
          (1 minute) -> BigDecimal(400),
          (3 minutes) -> BigDecimal(1000)
        ),
        delays = Member.Delay(callHandling = 15 minutes),
        scheduler = scheduler
      )
    )

  val member2 =
    system.actorOf(
      Member.props(
        name = "member 2",
        assets = Map(
          (0 minutes) -> BigDecimal(100),
          (1 minute) -> BigDecimal(400),
          (3 minutes) -> BigDecimal(1000)
        ),
        delays = Member.Delay(callHandling = 15 minutes),
        scheduler = scheduler
      )
    )

  val member3 =
    system.actorOf(
      Member.props(
        name = "member 3",
        assets = Map.empty[Time, BigDecimal],
        delays = Member.Delay(callHandling = 15 minutes),
        scheduler = scheduler
      )
    )

  val member4 =
    system.actorOf(
      Member.props(
        name = "member 4",
        assets = Map(
          (0 minutes) -> BigDecimal(100),
          (1 minute) -> BigDecimal(400),
          (3 minutes) -> BigDecimal(1000)
        ),
        delays = Member.Delay(callHandling = 15 minutes),
        scheduler = scheduler
      )
    )

  lazy val ccp1: ActorRef = system.actorOf(
    Ccp.props[Security](
      name = "ccp1",
      memberPortfolios = Map(
//        member1 -> longPortfolio,
        member3 -> longPortfolio,
        member4 -> shortPortfolio
      ),
      ccpPortfolios = Map.empty[ActorRef, Portfolio], // Map(ccp2 -> longPortfolio),
      assets = Map((0 minutes) -> BigDecimal(100),
                   (1 minute) -> BigDecimal(400),
                   (3 minutes) -> BigDecimal(1000)),
      rules = Rules(
        callEvery = 1 hour,
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
        callHandling = 15 minutes,
        coverWithDefaultedMargin = 15 minutes,
        coverWithDefaultedFund = 15 minutes,
        coverWithSurvivorsMargins = 15 minutes,
        coverWithSurvivorsFunds = 15 minutes,
        computeSurvivorsUnfunded = 15 minutes,
        coverWithSurvivorsUnfunded = 15 minutes,
        coverWithFirstLevelEquity = 15 minutes,
        coverWithSecondLevelEquity = 15 minutes
      ),
      scheduler = scheduler
    )
  )

  lazy val ccp2: ActorRef = system.actorOf(
    Ccp.props[Security](
      name = "ccp2",
      memberPortfolios =
        Map(member1 -> longPortfolio, member3 -> longPortfolio, member4 -> shortPortfolio),
      ccpPortfolios = Map(ccp2 -> longPortfolio),
      assets = Map((0 minutes) -> BigDecimal(100),
                   (1 minute) -> BigDecimal(400),
                   (3 minutes) -> BigDecimal(1000)),
      rules = Rules(
        callEvery = 1 hour,
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
        callHandling = 15 minutes,
        coverWithDefaultedMargin = 15 minutes,
        coverWithDefaultedFund = 15 minutes,
        coverWithSurvivorsMargins = 15 minutes,
        coverWithSurvivorsFunds = 15 minutes,
        computeSurvivorsUnfunded = 15 minutes,
        coverWithSurvivorsUnfunded = 15 minutes,
        coverWithFirstLevelEquity = 15 minutes,
        coverWithSecondLevelEquity = 15 minutes
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
    Scenario.props(ccps = Set(ccp1, ccp2), timeHorizon = 1 day, scheduler = scheduler)
  )

  Thread.sleep(2000)

  scenario.tell(Run, null)

//  Thread.sleep(10)
//
//  // Request paid
//  val member1PaidF: Future[BigDecimal] = ask(member1, Paid).mapTo[BigDecimal]
//  val member2PaidF: Future[BigDecimal] = ask(member2, Paid).mapTo[BigDecimal]
//  val member3PaidF: Future[BigDecimal] = ask(member3, Paid).mapTo[BigDecimal]
//  val member4PaidF: Future[BigDecimal] = ask(member4, Paid).mapTo[BigDecimal]
//  val ccp1PaidF: Future[BigDecimal] = ask(ccp1, Paid).mapTo[BigDecimal]
////  val ccp2PaidF: Future[BigDecimal] = ask(ccp2, Paid).mapTo[BigDecimal]
//
//  for {
//    member1Paid <- member1PaidF
//    member2Paid <- member2PaidF
//    member3Paid <- member3PaidF
//    member4Paid <- member4PaidF
//    ccp1Paid <- ccp1PaidF
////    ccp2Paid <- ccp2PaidF
//  } yield {
//    println(s"""Member 1 paid: ${member1Paid.toDouble}
//             |Member 2 paid: ${member2Paid.toDouble}
//             |Member 3 paid: ${member3Paid.toDouble}
//             |Member 4 paid: ${member4Paid.toDouble}
//             |CCP 1 paid: ${ccp1Paid.toDouble}
//           """.stripMargin)
//  }
}

//CCP 2 paid: ${ccp2Paid.toDouble}
