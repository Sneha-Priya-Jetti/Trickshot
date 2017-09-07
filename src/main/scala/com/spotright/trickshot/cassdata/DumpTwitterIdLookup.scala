package com.spotright.trickshot.cassdata

import com.spotright.common.util.LogHelper
import com.spotright.trickshot.spark.CanSparkContext
import com.spotright.xcass._
import com.spotright.xcass.spark.CanCass
import org.apache.spark.SparkContext

object DumpTwitterIdLookup extends CanSparkContext with CanCass with LogHelper {

  def main(args: Array[String]) {
    val output = args.lift(0).getOrElse {
      s"hdfs://c301.va.spotright.com:8020/user/nutch/TwitterIdLookup-${System.currentTimeMillis()}"
    }

    withSparkContext("DumpTwitterIdLookup") {
      sc =>
        worker(sc, output)
    }
  }

  def worker(sc: SparkContext, output: String) = {

    implicit val isc = sc

    createCassRDD(SpotCass.twitterIdLookup, where = "column1='handle'").map {
      case (key, columns) =>
        // head is safe because of predicate
        s"$key,${columns.head.stringVal}"
    }
      .saveAsTextFile(output)
  }
}
