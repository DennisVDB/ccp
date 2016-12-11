package util

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Created by dennis on 12/10/16.
 */
object DataUtil {
  def mapFToFMap[A, B](m: TrieMap[A, Future[B]]): Future[TrieMap[A, B]] = {
    Future.traverse(m) {
      case (a, fb) => fb.map((a, _))
    }.map(xs => TrieMap(xs.toSeq: _*))
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
}
