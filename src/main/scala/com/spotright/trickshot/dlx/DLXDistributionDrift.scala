package com.spotright.trickshot.dlx

import com.spotright.trickshot.spark.CanSparkContext
import org.apache.spark.SparkContext

/**
 * Created by sjetti on 2/14/17.
 */
object DLXDistributionDrift extends CanSparkContext {

  case class Opts(oldFile: String = "", newFile: String = "", outputFile: String = "")

  val parser = new scopt.OptionParser[Opts]("DLX Distribution Drift Finder") {

    arg[String]("<oldfile>") action { (x, c) =>
      c.copy(oldFile = x)
    } text "Path to old distribution File"
    arg[String]("<newfile>") action { (x, c) =>
      c.copy(newFile = x)
    } text "Path to new distribution File"

    arg[String]("<outputfile>") action { (x, c) =>
      c.copy(outputFile = x)
    } text "Path to Output File"
  }

  def main(args: Array[String]): Unit = {
    val opts = parser.parse(args, Opts()).get
    withSparkContext("DLX Distribution Drift") {
      sc =>
        distributionDrift(opts, sc)
    }
  }

  def distributionDrift(opts: Opts, sc: SparkContext): Unit = {

    implicit val isc = sc

    val oldData = sc.textFile(opts.oldFile, 3072).map {
      line =>
        val row = line.split(",")
        val count = if(row(3).replaceAll("\"", "").nonEmpty) row(3).replaceAll("\"", "").toDouble else 0.0
        s"${row(1)}:${row(2)}" -> count
    }

    val newData = sc.textFile(opts.newFile, 3072).map {
      line =>
        val row = line.split(",")
        val count = if(row(3).replaceAll("\"", "").nonEmpty) row(3).replaceAll("\"", "").toDouble else 0.0
        s"${row(1)}:${row(2)}" -> count
    }

    oldData.cogroup(newData).map{
      case (key, (oldCount, newCount)) =>

        val oldValue = if(oldCount.isEmpty) 0.0 else oldCount.head
        val newValue = if(newCount.isEmpty) 0.0 else newCount.head

        val diff =  newValue - oldValue
        val ratio = (diff/oldValue)*100
        val diffRatio = if(ratio.isNaN || ratio.isInfinite || ratio.isNegInfinity) ratio
        else BigDecimal(ratio).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

        val abs = scala.math.abs(diffRatio)

        s"$key,$oldValue,$newValue,$diff,$diffRatio%,$abs"
    }
      .coalesce(1)
    .saveAsTextFile(s"${opts.outputFile}/DistributionDrift.csv")

  }
}
