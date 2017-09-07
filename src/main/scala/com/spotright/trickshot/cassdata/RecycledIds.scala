package com.spotright.trickshot.cassdata

import com.spotright.common.lib.NsvString
import com.spotright.common.util.LogHelper
import com.spotright.trickshot.spark.CanSparkContext
import com.spotright.trickshot.util.Config
import com.spotright.xcass._
import com.spotright.xcass.spark._
import org.apache.spark.SparkContext
import scopt._


/**
  */


object RecycledIds extends CanSparkContext with CanCass with LogHelper {

  def worker(sc: SparkContext, opts: Opts) {

    implicit val isc = sc
    val RECORDS = longAccumulator("records")
    val HANDLES_IDS = longAccumulator("handles with multiple ids")

    createCassRDD(SpotCass.twitter, where = "column1 = 'id'")
      .filter { case (_, columns) =>
        RECORDS.add(1)
        columns.length > 1
      }
      .map { case (key, columns) =>
        HANDLES_IDS.add(1)
        // Compiler was having a hard time determining `String` for the s"" structure.
        // Using a dummy val to help with type resolution.
        val v = s"$key,${columns.map(_.stringVal).mkString(",")}"
        v
      }
      .saveAsTextFile(opts.output)
  }

  def zapEdges(srcHandle: String, tgtHandle: String)(implicit session: Session): Seq[StmtExec] = {

    val srcUrl = s"https://twitter.com/$srcHandle"
    val tgtUrl = s"https://twitter.com/$tgtHandle"

    // see Aragog.util.XfnEdgeWriter
    // see Merlin GraphBuilderEdgeII
    Seq(
      SpotCass.twitter.deleteCol(R2(srcHandle, "Out", tgtHandle)),
      SpotCass.twitter.deleteCol(R2(tgtHandle, "In", srcHandle)),

      SpotCass.links.deleteCol(R1(srcUrl, NsvString("e_Out", tgtUrl).toString())),
      SpotCass.links.deleteCol(R1(tgtUrl, NsvString("e_In", srcUrl).toString())),
      SpotCass.links.deleteCol(R1(tgtUrl, NsvString("eNew_In", srcUrl).toString()))
    )
  }

  case class Opts(output: String = s"${Config.hdfs}/user/nutch/RecycledIds-${System.currentTimeMillis()}")

  val parser = new OptionParser[Opts]("RecycledIds") {

    opt[String]("output") optional() action {
      (x, c) =>
        c.copy(output = x)
    }
  }

  def main(args: Array[String]) {
    val opts = parser.parse(args, Opts()).get
    withSparkContext("RecycleIds") {
      sc =>
        worker(sc, opts)
    }
  }
}
