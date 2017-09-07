package com.spotright.trickshot.dlx

import com.spotright.common.util.LogHelper
import com.spotright.search.client.SpotSolr
import com.spotright.trickshot.spark.CanSparkContext
import org.apache.spark.SparkContext

/**
 * Created by sjetti on 2/21/17.
 */
object DLXRegression extends CanSparkContext with LogHelper{

  case class Opts(oldFile: String = "", newFile: String = "", queries: String = "", outputFile: String = "")

  val parser = new scopt.OptionParser[Opts]("Regression Test For DLX Drift") {

    arg[String]("<oldfile>") action { (x, c) =>
      c.copy(oldFile = x)
    } text "Path to old distribution File"
    arg[String]("<newfile>") action { (x, c) =>
      c.copy(newFile = x)
    } text "Path to new distribution File"

    arg[String]("<queries>") action { (x, c) =>
      c.copy(queries = x)
    } text "Path to CSV header"

    arg[String]("<outputfile>") action { (x, c) =>
      c.copy(outputFile = x)
    } text "Path to Output File"
  }

  def main(args: Array[String]): Unit = {
    val opts = parser.parse(args, Opts()).get
    withSparkContext("DLX Regression Test") {
      sc =>
        dirDrift(opts, sc)
    }
  }

  def dirDrift(opts: Opts, sc: SparkContext): Unit = {
    implicit val isc = sc

    val results = scala.io.Source.fromFile(opts.queries).getLines().map {
      line =>
        val values = line.split("->")

        val queryString = values.head
        val expectedValue = values.last

        val newCollValue = query(opts.newFile, queryString)
        val oldCollValue = query(opts.oldFile, queryString)
        val diffRatio = (newCollValue - oldCollValue).toDouble * 100 / oldCollValue.toDouble
        val ratio = if(diffRatio.isNaN || diffRatio.isInfinite || diffRatio.isNegInfinity) diffRatio.toDouble
        else BigDecimal(diffRatio).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

        if(ratio > expectedValue.toDouble) logger.error(s" Ratio $ratio is greater than expected Value $expectedValue for $queryString ")
        s"$queryString\t$oldCollValue\t$newCollValue\t$ratio%\t$expectedValue"
    }
      .toList

    sc.parallelize(results).coalesce(1).saveAsTextFile(s"${opts.outputFile}/RegressionTest")
  }

  def query(coll: String, line: String): Int = {

    val sq = SpotSolr.solrQuery(coll, "/select")
      .setQuery(s"$line")
      .setRows(0)
    SpotSolr.withServer(_.query(sq)).getResults.getNumFound.toInt
  }
}
