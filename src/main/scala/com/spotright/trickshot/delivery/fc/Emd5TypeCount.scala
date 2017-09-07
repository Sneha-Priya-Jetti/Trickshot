package com.spotright.trickshot.delivery.fc

import com.spotright.common.util.LogHelper
import com.spotright.trickshot.spark.CanSparkContext
import org.apache.spark.SparkContext
import spray.json._

/**
  * Created by sjetti on 9/12/16.
  */
object Emd5TypeCount extends CanSparkContext with LogHelper {

  final val fcPartitions = 3072

  final val empty = Seq.empty[String]

  case class Opts(hdfsPath: String = "", outputFile: String = "")

  val optParser = new scopt.OptionParser[Opts]("Emd5TypeCount") {
    arg[String]("<hdfsPath>") text "HDFS URI to latest FC file" action { case (p, c) => c.copy(hdfsPath = p) }
    arg[String]("<outputPath>") text "Output location to sace files" action { case (p, c) => c.copy(outputFile = p) }
  }

  def main(av: Array[String]): Unit = {
    optParser.parse(av, Opts()).foreach {
      c =>
        if (c.hdfsPath.isEmpty || !c.hdfsPath.startsWith("hdfs://"))
          sys.error(s"bad URI: ${c.hdfsPath}")
        else {
          withSparkContext("FC Count Social Profile types") {
            sc =>
              spark(c, sc)
          }
        }
    }
  }

  def spark(opts: Opts, sc: SparkContext) = {
    implicit val isc = sc

    val results = sc.textFile(opts.hdfsPath, minPartitions = fcPartitions).flatMap {
      line =>
        overlap(line)
    }.map { word => (word, 1) }
      .reduceByKey(_ + _)
      .collect()
      .toMap

    val percentage = results.flatMap {
      case (sp, count) =>
        if (sp.contains("_")) {
          Some("per_" + sp -> calc(count, results("records")))
        }
        else None
    }

    sc.parallelize(results.toSeq)
      .coalesce(1)
      .saveAsTextFile(s"${opts.outputFile}/fclatest-sp-counts")

    sc.parallelize(percentage.toSeq)
      .coalesce(1)
      .saveAsTextFile(s"${opts.outputFile}/fclatest-sp-percentage")

  }

  def overlap(line: String) = {

    var counters = Seq[String]()
    counters :+= "records"

    line.split("\t", 4) match {
      case Array(_, _, emd5, jstext) =>
        jstext.parseJson match {
          case JsObject(fs) =>
            fs.get("socialProfiles") match {
              case Some(JsArray(xs)) =>

                val twit = xs.collect {
                  case JsObject(gs) =>
                    gs.get("type") match {
                      case Some(JsString(social)) =>
                        counters :+= social
                        social
                      case None =>
                        counters :+= "NONE"
                        "NONE"
                      case _ => sys.error("Not a JsString")
                    }
                }

                twit.toList.distinct.sorted.combinations(2).foreach {
                  x =>
                    counters :+= s"${x.head}-${x.last}"
                }

                twit.distinct.foreach {
                  social => counters :+= s"has_$social"
                }

              case _ =>
                counters :+= "no-sp"
            }
          case _ => sys.error(s"$emd5 - json is not a JsObject")
        }
      case _ =>
    }

    counters
  }

  def calc(count: Int, records: Int): Float = {
    count * 100 / records.toFloat
  }
}
