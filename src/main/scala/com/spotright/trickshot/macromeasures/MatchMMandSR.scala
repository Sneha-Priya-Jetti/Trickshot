package com.spotright.trickshot.macromeasures

import com.opencsv.CSVParser
import com.spotright.trickshot.spark.CanSparkContext
import com.spotright.trickshot.util.Config
import com.spotright.trickshot.util.HelperMethods._
import com.spotright.xcass.spark._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext

/**
 * Created by sjetti on 3/15/17.
 */
object MatchMMandSR extends CanSparkContext with CanCass {

  case class Opts(inputFile: String = "", outputDir: String = "")

  val parser = new scopt.OptionParser[Opts]("WilandCandidateAnalyzer") {
    arg[String]("<inputfile>") action { (x, c) =>
      c.copy(inputFile = x)
    } text "Input Wiland Scored File"

    arg[String]("<outputdir>") action { (x, c) =>
      c.copy(outputDir = x)
    } text "Path to CSV header"
  }

  def main(args: Array[String]): Unit = {

    val opts = parser.parse(args, Opts()).get
    withSparkContext("Handles Match") {
      sc =>
        matchHandles(opts, sc)
    }
  }

  def matchHandles(opts: Opts, sc: SparkContext): Unit = {

    implicit val isc = sc
    val cassConn = CassandraConnector(sc.getConf)

    val srHandles = sc.textFile(s"${Config.hdfs}/user/nutch/Insta/sr-categories.csv", 3072).mapPartitions {
      iter => cassConn.withSessionDo { implicit session =>

        iter.map {
          line =>
            val row = line.split(",")
            val name = getTwitterMetaSafely(row.head).name.getOrElse("Not Defined").replaceAll("[^\\x00-\\x7F]", "")
            row.head -> (name -> row.last)
        }
      }
    }
      .collect()
      .toSeq

    val mmHandles = sc.textFile(s"${opts.inputFile}", 3072).map {
      line =>
        val parser = new CSVParser()
        val row = parser.parseLine(line)
        val category = row(row.length - 1)
        val title = row(row.length - 2).replaceAll("\"", "")
        title -> category
    }
      .distinct()

    mmHandles.saveAsTextFile(s"${opts.outputDir}/InterestTrails-Distinct")

    val mm = mmHandles.map {
      case (title, category) =>
        title.toLowerCase -> (title -> category)
    }

    mm.map {
      case (trimmed, (title, iCategory)) =>
        srHandles.flatMap {
          case (handle, (name, srCategory)) =>
            val titleDistance = StringUtils.getJaroWinklerDistance(title.toLowerCase, name.toLowerCase)
            val handleDistance = StringUtils.getJaroWinklerDistance(trimmed.toLowerCase, handle.toLowerCase)
            if (trimmed.replaceAll("[^A-Za-z0-9_]", "") == handle) {
              Some(s"$title,$trimmed,$iCategory,$handle,$name,$srCategory,$handleDistance,$titleDistance,Squish Match")
            }
            else if (handleDistance > 0.8 || titleDistance > 0.8)
              Some(s"$title,$trimmed,$iCategory,$handle,$name,$srCategory,$handleDistance,$titleDistance")
            else None
        }
    }
      .flatMap(identity)
      .distinct()
      .saveAsTextFile(s"${opts.outputDir}/JaroWinklerTitleMatch")
  }
}
