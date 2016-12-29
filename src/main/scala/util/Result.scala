package util

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scalaz.Scalaz._
import scalaz._

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

  def collect[A](a: Result[A]): Future[A] = a.run.collect {
    case Some(aa) => aa
    case None => throw new IllegalStateException("Could not collect.")
  }

  implicit val resultFunctor = new Functor[Result] {
    def map[A, B](fa: Result[A])(f: A => B): Result[B] = fa map f
  }

  implicit def resultMonoid[A: Monoid]: Monoid[Result[A]] = new Monoid[Result[A]] {
    def zero: Result[A] = Monoid[A].zero.point[Result]
    def append(f1: Result[A], f2: => Result[A]): Result[A] = (f1 |@| f2) { _ |+| _ }
  }
}
