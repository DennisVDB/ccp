package util

import scalaz._

/**
  * Created by dennis on 26/12/16.
  */
object Result {
  type Result[A] = String \/ A

  def pure[A](a: A): Result[A] = \/-(a)
  def flatten[A] (a: String \/ Result[A]): Result[A] = a match {
    case le @ -\/(_) => le
    case \/-(r) =>
      r match {
        case re @ -\/(_) => re
        case ra @ \/-(_) => ra
      }
  }

//  type E = String
//  type Result[A] = EitherT[Future, E, A]
//
//  def apply[A](a: Future[E \/ A]): Result[A] = EitherT(a)
//
//  def fromOption[A](a: E \/ A): Result[A] = EitherT(a.point[Future])
//
//  def fromFuture[A](a: Future[A]): Result[A] =
//    EitherT[Future, E, A](a.map(\/-(_)))
//
//  def fromOptFut[A](a: E \/ Future[A]): Result[A] = EitherT(a.sequence)
//
//  def fromOptRes[A](a: E \/ Result[A]): Result[A] =
//    EitherT(a.traverse(_.run).map {
//      case -\/(e) => -\/(e)
//      case \/-(x) =>
//        x match {
//          case -\/(ee) => -\/(ee)
//          case \/-(a) => \/-(a)
//        }
//    })
//
////  EitherT(a.map(_.run).sequence.map(_.flatten))
//
//  def fromFutureRes[A](a: Future[Result[A]]): Result[A] =
//    Result(a.map(_.run).flatMap(identity))
//
//  def pure[E, A](a: A): Result[A] = a.point[Result]
//
//  def unsafeCollect[E, A](a: Result[A]): Future[A] = a.run.collect {
//    case \/-(a) => a
//    case -\/(e) => throw new IllegalStateException(e.toString())
//  }
//
////  implicit val resultFunctor = new Functor[Result] {
////    def map[A, B](fa: Result[A])(f: (A) => B): Result[B] = ???
////  }
//
//  implicit def resultMonoid[A: Monoid]: Monoid[Result[A]] = new Monoid[Result[A]] {
//    def zero: Result[A] = Monoid[A].zero.point[Result]
//
//    def append(f1: Result[A], f2: => Result[A]): Result[A] = (f1 |@| f2) { _ |+| _ }
//  }
}
