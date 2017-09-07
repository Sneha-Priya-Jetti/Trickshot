package com.spotright.trickshot.delivery.fc

import com.spotright.common.util.{InputTidy, LogHelper}
import com.spotright.trickshot.spark.CanSparkContext
import org.apache.spark.util.LongAccumulator
import org.apache.spark.SparkContext
import spray.json._

/**
  * Output to dfs is
  * emailMd5,handle,id
  *
  * cat fc_twids | awk -F, '{if ($2 != "") print $1","$2}' > fullcontact_twitter_handles
  * cat fc_twids | awk -F, '{if ($3 != "") print $1","$3}' > fullcontact_twids
  */


object EmitEmd5TwitterHandle extends CanSparkContext with LogHelper {

  // As of May 2015 FC file was 161M lines.  Keep partitions < 64k lines.
  final val fcPartitions = 3072
  // 1024 * 3
  final val empty = Seq.empty[String]

  case class Opts(hdfsPath: String)

  val optParser = new scopt.OptionParser[Opts]("EmitEmd5TwitterHandle") {
    arg[String]("<hdfsPath>") text "HDFS URI to latest FC file" action { case (p, c) => c.copy(hdfsPath = p) }
  }

  def main(av: Array[String]): Unit = {
    optParser.parse(av, Opts("")).foreach {
      c =>
        if (c.hdfsPath.isEmpty || !c.hdfsPath.startsWith("hdfs://"))
          sys.error(s"bad URI: ${c.hdfsPath}")
        else {
          withSparkContext("FC Emit Emd5,TwitterHandle,Id") {
            sc =>
              spark(c.hdfsPath, sc)
          }
        }
    }
  }

  def spark(hdfsURI: String, sc: SparkContext): Unit = {

    implicit val isc = sc

    val counters = Map(
      "records" -> counter("records"),
      "no_sp" -> counter("no_social_profiles"),
      "has_twitter" -> counter("has_twitter"),
      "no_twitter" -> counter("no_twitter"),
      "emd5_hand_twid_tuples" -> counter("md5_hand_id_tuples"),
      "0_twitters" -> counter("0_twitters"),
      "1_twitters" -> counter("1_twitters"),
      "2_twitters" -> counter("2_twitters"),
      "n_twitters" -> counter("n_twitters"),
      "no_id" -> counter("no_id")
    )

    sc.textFile(hdfsURI, minPartitions = fcPartitions).flatMap {
      line =>
        parseFCLine(line, counters)
    }.coalesce(fcPartitions / 2)
      .saveAsTextFile("hdfs://c301.va.spotright.com:8020/user/nutch/fclatest-emd5-handle")

    // dump counters
    counters.foreach {
      case (name, acc) =>
        logger.info(s"$name: ${acc.value}")
    }
  }

  def parseFCLine(line: String, counters: Map[String, LongAccumulator] = Map()): Seq[String] = {
    // FC format is (emd5, json)
    // The json is an object which may contain "socialProfiles" which is an array of profiles
    // Profiles of type "twitter" are examined for a "username" and if found (emd5, username, id) is emitted.

    counters.get("records").foreach(_ add 1)

    line.split("\t", 4) match {
      case Array(_, _, emd5, jstext) =>
        jstext.parseJson match {
          case JsObject(fs) =>
            fs.get("socialProfiles") match {
              case Some(JsArray(xs)) =>

                val twits =
                  xs.filter {
                    case JsObject(gs) if gs.get("type").contains(JsString("twitter")) =>
                      counters.get("has_twitter").foreach(_ add 1)
                      true
                    case x =>
                      counters.get("no_twitter").foreach(_ add 1)
                      false
                  }

                val ret =
                  twits.collect {
                    case JsObject(gs) if gs.contains("username") || gs.contains("id") =>
                      val hand =
                        gs.get("username") match {
                          case Some(JsString(handle)) => InputTidy.normalizeTwitterHandle(handle).getOrElse("")
                          case None => ""
                          case _ => sys.error("Not a JsString")
                        }

                      val twid =
                        gs.get("id") match {
                          case Some(JsString(id)) => id
                          case None =>
                            counters.get("no_id").foreach(_ add 1)
                            ""
                          case _ => sys.error("Not a JsString")
                        }

                      counters.get("emd5_hand_twid_tuples").foreach(_ add 1)
                      s"$hand,$twid"
                  }


                ret.length match {
                  case 0 => counters.get("0_twitters").foreach(_ add 1)
                  case 1 => counters.get("1_twitters").foreach(_ add 1)
                  case 2 => counters.get("2_twitters").foreach(_ add 1)
                  case _ => counters.get("n_twitters").foreach(_ add 1)
                }

                if (ret.isEmpty)
                  Seq(s"$emd5,,")
                else
                  ret.map { v => s"$emd5,$v" }

              // jsObject did not contain key "socialProfiles"
              case _ =>
                counters.get("no_sp").foreach(_ add 1)
                empty
            }

          case _ => sys.error(s"$emd5 - json is not a JsObject")
        }

      case _ =>
        //val len = if (line.isEmpty) 0 else 63 min (line.length - 1)
        //sys.error(s"${line.substring(0, len)} - line not composed of (_, _, emd5, json)")
        empty
    }
  }
}
