package com.spotright.trickshot.dlx

import com.spotright.trickshot.spark.CanSparkContext
import org.apache.spark.SparkContext

/**
 * Created by sjetti on 3/14/17.
 */
object ValidateDLXRecord extends CanSparkContext {

  case class Opts(currentMonth: String = "", previousMonth: String = "", output: String = "")

  val parser = new scopt.OptionParser[Opts]("Readable Distribution") {
    arg[String]("<currentmonth>") action { (x, c) =>
      c.copy(currentMonth = x)
    } text "Path to current month distribution File"

    arg[String]("<previousmonth>") action { (x, c) =>
      c.copy(previousMonth = x)
    } text "Path to display Fields File"

    arg[String]("<output>") action { (x, c) =>
      c.copy(output = x)
    } text "Path to display Fields File"
  }

  def main(args: Array[String]): Unit = {
    val opts = parser.parse(args, Opts()).get
    withSparkContext("Distribution") {
      sc =>
        dirDrift(opts, sc)
    }
  }

  def dirDrift(opts: Opts, sc: SparkContext): Unit = {
    implicit val isc = sc

    sc.textFile(s"${opts.currentMonth}", 3072).flatMap {
      newLine =>
        val line = newLine.split(",").dropRight(1).mkString(",")
        val dlxLine = line.split(",")
        if (dlxLine.tail.isEmpty) Some(dlxLine.head)
        else None
    }.saveAsTextFile(s"${opts.output}/LatestEmptyFiles")

    sc.textFile(s"${opts.previousMonth}", 3072).flatMap {
      newLine =>
        val line = newLine.split(",").dropRight(1).mkString(",")
        val dlxLine = line.split(",")
        if (dlxLine.tail.nonEmpty) Some(dlxLine.head)
        else None
    }.saveAsTextFile(s"${opts.output}/OldNonEmptyFiles")
  }
}
