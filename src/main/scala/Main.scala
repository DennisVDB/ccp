import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.MultivariateGaussian
import com.typesafe.config.ConfigFactory
import market.{Market, Portfolio, Security}
import structure.Ccp._
import structure.Scenario.Run
import structure.Timed.Time
import structure.{Ccp, Member, Scenario, Scheduler}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._

/**
  * Created by dennis on 8/10/16.
  */
object Main extends App {

  implicit val timeout: Timeout = Timeout(60 seconds)

  //  override def main(args: Array[String]): Unit = {
  val system = ActorSystem("System", ConfigFactory.load())

  val pos1 = Security("pos1")
  val pos2 = Security("pos2")

  //
  //  implicit val market = Market[Security](
  //    TrieMap[Security, PriceData](
  //      (pos1, PriceData(BigDecimal("100"), Gaussian(0.05, 0.2)))
  //    )
  //  )

  implicit val market = Market[Security](
    TrieMap(pos1 -> BigDecimal("100"), pos2 -> BigDecimal("50")),
    Map(pos1 -> 0, pos2 -> 1),
    MultivariateGaussian(
      DenseVector(0.05, 0.25),
      DenseMatrix((0.25, 0.0), (0.0, 0.5))
    )
  )

  val longPortfolio: Portfolio[Security] =
    Portfolio(positions = Map((pos1, 1)), liquidity = 1)

  val shortPortfolio = longPortfolio.inverse

  val emptyPortfolio =
    Portfolio(positions = Map.empty[Security, Long], liquidity = 0)

  implicit val scheduler = system.actorOf(Scheduler.props(100 milliseconds))

  val member1 =
    system.actorOf(
      Member.props(
        name = "member 1",
        assets = Map(
          0 -> BigDecimal(100),
          1 -> BigDecimal(400),
          3 -> BigDecimal(1000)
        ),
        scheduler = scheduler
      )
    )

  val member2 =
    system.actorOf(
      Member.props(
        name = "member 2",
        assets = Map(
          0 -> BigDecimal(100),
          1 -> BigDecimal(400),
          3 -> BigDecimal(1000)
        ),
        scheduler = scheduler
      )
    )

  val member3 =
    system.actorOf(
      Member.props(
        name = "member 3",
        assets = Map.empty[Time, BigDecimal],
        scheduler = scheduler
      )
    )

  val member4 =
    system.actorOf(
      Member.props(
        name = "member 4",
        assets = Map(
          0 -> BigDecimal(100),
          1 -> BigDecimal(400),
          3 -> BigDecimal(1000)
        ),
        scheduler = scheduler
      )
    )

  lazy val ccp1: ActorRef = system.actorOf(
    Ccp.props[Security](
      name = "ccp1",
      memberPortfolios = Map(
        member1 -> longPortfolio,
        member3 -> longPortfolio,
        member4 -> shortPortfolio
      ),
      ccpPortfolios = Map(ccp2 -> shortPortfolio),
      assets = Map(0 -> BigDecimal(100), 1 -> BigDecimal(400), 3 -> BigDecimal(1000)),
      rules = Rules(
        marginCallFrequency = 1,
        maxDelay = 8,
        minimumTransfer = BigDecimal(0),
        marginIsRingFenced = false,
        maximumFundCall = BigDecimal(1000),
        skinInTheGame = BigDecimal("0.1"),
        memberRules = MemberRules(
          marginCoverage = BigDecimal("0.95"),
          fundParticipation = BigDecimal("0.2")
        ),
        ccpRules = CcpRules(
          participatesInMargin = true,
          marginCoverage = BigDecimal("0.95"),
          fundParticipation = BigDecimal("0.2")
        )
      ),
      operationalDelays = OperationalDelays(
        coverWithDefaultingMember = 0,
        coverWithNonDefaultingMembers = 0,
        coverWithNonDefaultingUnfundedFunds = 0,
        coverWithFirstLevelEquity = 0,
        coverWithSecondLevelEquity = 0
      ),
      scheduler = scheduler
    )
  )

  lazy val ccp2: ActorRef = system.actorOf(
    Ccp.props[Security](
      name = "ccp2",
      memberPortfolios = Map(
        member1 -> longPortfolio,
        member3 -> longPortfolio,
        member4 -> shortPortfolio
      ),
      ccpPortfolios = Map(ccp2 -> shortPortfolio),
      assets = Map(0 -> BigDecimal(100), 1 -> BigDecimal(400), 3 -> BigDecimal(1000)),
      rules = Rules(marginCallFrequency = 1,
                    maxDelay = 8,
                    minimumTransfer = BigDecimal(0),
                    marginIsRingFenced = false,
                    maximumFundCall = BigDecimal(1000),
                    skinInTheGame = BigDecimal("0.1"),
                    memberRules = MemberRules(
                      marginCoverage = BigDecimal("0.95"),
                      fundParticipation = BigDecimal("0.2")
                    ),
                    ccpRules = CcpRules(
                      participatesInMargin = true,
                      marginCoverage = BigDecimal("0.95"),
                      fundParticipation = BigDecimal("0.2")
                    )),
      operationalDelays = OperationalDelays(
        coverWithDefaultingMember = 0,
        coverWithNonDefaultingMembers = 0,
        coverWithNonDefaultingUnfundedFunds = 0,
        coverWithFirstLevelEquity = 0,
        coverWithSecondLevelEquity = 0
      ),
      scheduler = scheduler
    )
  )

  println(s"ccp1 as $ccp1")
  println(s"ccp2 as $ccp2")
  println(s"member1 as $member1")
  println(s"member2 as $member2")
  println(s"member3 as $member3")
  println(s"member4 as $member4")

  val scenario = system.actorOf(
    Scenario
      .props(ccps = List(ccp1, ccp2), market = market, timeHorizon = 250, scheduler = scheduler)
  )

  Thread.sleep(2000)

  scenario.tell(Run, null)

  Thread.sleep(2000)

  //  // Request paid
  //  val member1PaidF: Future[BigDecimal] = ask(member1, Paid).mapTo[BigDecimal]
  //  val member2PaidF: Future[BigDecimal] = ask(member2, Paid).mapTo[BigDecimal]
  //  val member3PaidF: Future[BigDecimal] = ask(member3, Paid).mapTo[BigDecimal]
  //  val member4PaidF: Future[BigDecimal] = ask(member4, Paid).mapTo[BigDecimal]
  //  val ccp1PaidF: Future[BigDecimal] = ask(ccp1, Paid).mapTo[BigDecimal]
  //  val ccp2PaidF: Future[BigDecimal] = ask(ccp2, Paid).mapTo[BigDecimal]
  //
  //  for {
  //    member1Paid <- member1PaidF
  //    member2Paid <- member2PaidF
  //    member3Paid <- member3PaidF
  //    member4Paid <- member4PaidF
  //    ccp1Paid <- ccp1PaidF
  //    ccp2Paid <- ccp2PaidF
  //  } yield {
  //    println(s"""Member 1 paid: ${member1Paid.toDouble}
  //           |Member 2 paid: ${member2Paid.toDouble}
  //           |Member 3 paid: ${member3Paid.toDouble}
  //           |Member 4 paid: ${member4Paid.toDouble}
  //           |CCP 1 paid: ${ccp1Paid.toDouble}
  //           |CCP 2 paid: ${ccp2Paid.toDouble}
  //         """.stripMargin)
  //  }
  //  }
}
