import Ccp._
import CcpProtocol.Paid
import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.MultivariateGaussian
import com.typesafe.config.ConfigFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
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
    MultivariateGaussian(DenseVector(0.05, 0.25),
                         DenseMatrix((0.25, 0.0), (0.0, 0.5))))

  val longPortfolio = Portfolio(Map((pos1, 1)))
//  val shortPortfolio = longPortfolio.inverse
//  val emptyPortfolio = Portfolio(Map.empty[Security, Long])

  val member1 =
    system.actorOf(
      Member.props("member 1", longPortfolio, shouldDefault = true)
    )

  val member2 =
    system.actorOf(Member.props("member 2", longPortfolio))

  val member3 = system.actorOf(
    Member.props("member 3", longPortfolio, shouldDefault = true)
  )

  val member4 =
    system.actorOf(Member.props("member 4", longPortfolio))

  lazy val ccp1: ActorRef = system.actorOf(
    props[Security](
      "ccp1",
      Map(member1 -> longPortfolio,
          member3 -> longPortfolio,
          member4 -> longPortfolio),
      Map(ccp2 -> longPortfolio),
      equity = BigDecimal("10"),
      Rules(
        marginIsRingFenced = false,
        maximumFundCall = 1000,
        minimumTransfer = 0,
        MemberRules(marginCoverage = BigDecimal("0.95"),
                    fundParticipation = BigDecimal("0.2")),
        CcpRules(participatesInMargin = true,
                 marginCoverage = BigDecimal("0.95"),
                 fundParticipation = BigDecimal("0.2"))
      ),
      shouldDefault = false
    )
  )

  lazy val ccp2: ActorRef = system.actorOf(
    props[Security](
      "ccp2",
      Map(member2 -> longPortfolio),
      Map(ccp1 -> longPortfolio),
      equity = BigDecimal("100000"),
      Rules(
        marginIsRingFenced = true,
        maximumFundCall = 1000,
        minimumTransfer = 0,
        MemberRules(marginCoverage = BigDecimal("0.95"),
                    fundParticipation = BigDecimal("0.2")),
        CcpRules(participatesInMargin = true,
                 marginCoverage = BigDecimal("0.95"),
                 fundParticipation = BigDecimal("0.2"))
      )
    )
  )

  println(s"ccp1 as $ccp1")
  println(s"ccp2 as $ccp2")
  println(s"member1 as $member1")
  println(s"member2 as $member2")
  println(s"member3 as $member3")
  println(s"member4 as $member4")

  val scenario = system.actorOf(Scenario.props(List(ccp1, ccp2), market))

  Thread.sleep(2000)

  scenario.tell(TriggerMarginCalls, null)

  Thread.sleep(2000)

  // Request paid
  val member1PaidF: Future[BigDecimal] = ask(member1, Paid).mapTo[BigDecimal]
  val member2PaidF: Future[BigDecimal] = ask(member2, Paid).mapTo[BigDecimal]
  val member3PaidF: Future[BigDecimal] = ask(member3, Paid).mapTo[BigDecimal]
  val member4PaidF: Future[BigDecimal] = ask(member4, Paid).mapTo[BigDecimal]
  val ccp1PaidF: Future[BigDecimal] = ask(ccp1, Paid).mapTo[BigDecimal]
  val ccp2PaidF: Future[BigDecimal] = ask(ccp2, Paid).mapTo[BigDecimal]

  for {
    member1Paid <- member1PaidF
    member2Paid <- member2PaidF
    member3Paid <- member3PaidF
    member4Paid <- member4PaidF
    ccp1Paid <- ccp1PaidF
    ccp2Paid <- ccp2PaidF
  } yield {
    println(s"""Member 1 paid: ${member1Paid.toDouble}
           |Member 2 paid: ${member2Paid.toDouble}
           |Member 3 paid: ${member3Paid.toDouble}
           |Member 4 paid: ${member4Paid.toDouble}
           |CCP 1 paid: ${ccp1Paid.toDouble}
           |CCP 2 paid: ${ccp2Paid.toDouble}
         """.stripMargin)
  }
//  }
}
