package com.spotright.trickshot.util

import java.time._
import java.time.format._
import java.util.concurrent.TimeUnit

import com.spotright.common.util.{InputTidy, SimpleCSV, UrlTidy}
import org.apache.spark.rdd.RDD

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try
import scalaz.Monoid

/**
  * Created by nhalko on 2/12/15.
  */


/**
  * ToDo - Move these to Common
  */
object Helpers {

  val hawkeye =
    """
      |______  __                ______
      |___  / / /_____ ___      ____  /___________  ______
      |__  /_/ /_  __ `/_ | /| / /_  //_/  _ \_  / / /  _ \
      |_  __  / / /_/ /__ |/ |/ /_  ,<  /  __/  /_/ //  __/
      |/_/ /_/  \__,_/ ____/|__/ /_/|_| \___/_\__, / \___/
      |                                      /____/
    """.stripMargin

  val tsFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  val ymdFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  def generateSegmentName(dt: OffsetDateTime = OffsetDateTime.now) = dt.format(tsFormatter)

  def generateDay(dt: OffsetDateTime = OffsetDateTime.now) = dt.format(ymdFormatter)

  // http://joda-time.sourceforge.net/apidocs/org/joda/time/format/ISODateTimeFormat.html
  def iso8601(): String = OffsetDateTime.now.toString

  def errorMsgAndStackTrace(e: Throwable): String = s"${e.getMessage}\n${e.getStackTrace.map(_.toString).mkString("\n")}"

  final val twitterURLPrefix = "https://twitter.com/"

  def handleToMaybeUrl(handle: String): Option[String] = {
    InputTidy.normalizeTwitterHandle(handle).flatMap {
      normHand =>
        UrlTidy.urlFilterThenNormalize(s"$twitterURLPrefix$normHand")
    }
  }

  /* Downstream code makes assumption that the handle might be empty.  Retain this form unless all
     usage of the call are fixed to accept None.
   */
  def handleToUrl(handle: String): String = handleToMaybeUrl(handle).getOrElse(twitterURLPrefix)

  @annotation.tailrec
  def unforeach[A](a: A)(f: A => Option[A]) {
    f(a) match {
      case Some(b) => unforeach[A](b)(f)
      case None => ()
    }
  }

  /** Use this to turn a Future[A] into an Option[A] after a given timeout */
  def fxWait[A](fx: Future[A], duration: Duration): Option[A] =
  Try {
    Await.result(fx, duration)
  }.toOption

  // Helper to deal with Duration collisions from joda time and conncurrent time
  def fxWait[A](fx: Future[A], seconds: Int = 30, minutes: Int = 0, hours: Int = 0): Option[A] = {
    val duration = Duration(seconds, TimeUnit.SECONDS) + Duration(minutes, TimeUnit.MINUTES) + Duration(hours, TimeUnit.HOURS)
    fxWait(fx, duration)
  }

  /** Use this to turn a Future[Option[A]] into an Option[A] after a given timeout */
  def foxWait[A](fx: Future[Option[A]], duration: Duration): Option[A] =
  Try {
    Await.result(fx, duration)
  }.toOption.flatten

  // Helper to deal with Duration collisions from joda time and conncurrent time
  def foxWait[A](fx: Future[Option[A]], seconds: Int = 30, minutes: Int = 0, hours: Int = 0): Option[A] = {
    val duration = Duration(seconds, TimeUnit.SECONDS) + Duration(minutes, TimeUnit.MINUTES) + Duration(hours, TimeUnit.HOURS)
    foxWait(fx, duration)
  }

  implicit class ListPad(val xs: List[String]) extends AnyVal {
    def pad(i: Int) = xs ++ List.fill(i - xs.length)("")
  }

  // copy/paste the Duration and the Succeeded/Total ratio of tasks from SparkUI to estimate job finish time
  def jobEnds(dur: Double, percentDone: Double) = {
    val hrsLeft = dur / percentDone - dur
    val secondsLeft = hrsLeft * 60 * 60
    OffsetDateTime.now.plusSeconds(secondsLeft.toInt)
  }

  // Allows use of BigDecimal as a monoid
  implicit object BDMonoid extends Monoid[BigDecimal] {
    val zero: BigDecimal = BigDecimal.apply(0L)

    def append(a: BigDecimal, b: => BigDecimal): BigDecimal = a + b
  }

  // Adds filterNot() to org.apache.spark.rdd.RDD
  implicit class RichRDD[T](val rdd: RDD[T]) extends AnyVal {
    def filterNot(pred: T => Boolean): RDD[T] = rdd.filter(!pred(_))
  }

  // Allows a Seq to be properly formatted as a CSV string
  implicit class CSVSeq[A](val list: Seq[A]) extends AnyVal {
    def asCSV: String = list.map(i => SimpleCSV.toCSV(i.toString)).mkString(",")
  }

  /**
    * Chain a sequence of transformations together
    *
    * Example:
    *
    *  chain(10)(x => x + 1, x => x * 3) = (10 + 1) * 3
    */
  def chain[A](init: A)(fx: (A => A)*): A = {
    fx.foldLeft(init){case (data, f) => f(data)}
  }
}
