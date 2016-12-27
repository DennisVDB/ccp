package util

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scalaz.std.option._
import scalaz.std.scalaFuture._
import scalaz.syntax.applicative._
import scalaz.syntax.traverse._
import scalaz.{Functor, Monoid, OptionT}

/**
  * Created by dennis on 26/12/16.
  */
object Result {
  type Result[A] = OptionT[Future, A]

  def apply[A](a: Future[Option[A]]): Result[A] = OptionT(a)

  def fromOption[A](a: Option[A]): Result[A] = OptionT(a.point[Future])

  def fromFuture[A](a: Future[A]): Result[A] = OptionT(a.map(Option(_)))

  def fromOptFut[A](a: Option[Future[A]]): Result[A] = OptionT(a.sequence)

  def fromOptRes[A](a: Option[Result[A]]): Result[A] =
    OptionT(a.map(_.run).sequence.map(_.flatten))

  def fromFutureRes[A](a: Future[Result[A]]): Result[A] = Result(a.map(_.run).flatMap(identity))

  def pure[A](a: A): Result[A] = a.point[Result]

  def collect[A](a: Result[A]): Future[A] = a.run.collect { case Some(aa) => aa }

  implicit val ResultFunctor = new Functor[Result] {
    def map[A, B](fa: Result[A])(f: A => B): Result[B] = fa map f
  }

  implicit def BigDecimalAdditionMonoid = new Monoid[BigDecimal] {
    def zero: BigDecimal = 0

    def append(f1: BigDecimal, f2: => BigDecimal): BigDecimal = f1 + f2
  }
}
