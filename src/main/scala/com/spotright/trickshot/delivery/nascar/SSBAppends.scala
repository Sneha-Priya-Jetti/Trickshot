package com.spotright.trickshot.delivery.nascar

import com.opencsv.CSVParser
import com.spotright.common.util.LogHelper
import com.spotright.trickshot.spark.CanSparkContext
import com.spotright.xcass.spark.CanCass
import org.apache.spark.SparkContext

/**
 * DLV-319
 * Created by sjetti on 3/8/17.
 */
object SSBAppends extends CanSparkContext with CanCass with LogHelper {

  case class Opts(inputFile: String = "", appendsFile: String = "", originalFile: String = "", indexesFile: String = "", outputDir: String = "")

  val parser = new scopt.OptionParser[Opts]("WilandCandidateAnalyzer") {
    arg[String]("<inputfile>") action { (x, c) =>
      c.copy(inputFile = x)
    } text "Acxiom Appends File"

    arg[String]("<appendsfile>") action { (x, c) =>
      c.copy(appendsFile = x)
    } text "SpotRight Appends File"

    arg[String]("<nascarfile>") action { (x, c) =>
      c.copy(originalFile = x)
    } text "SSB Appends File"

    arg[String]("<indexesfile>") action { (x, c) =>
      c.copy(indexesFile = x)
    } text "Indexes to fetch from Acxiom File"

    arg[String]("<outputdir>") action { (x, c) =>
      c.copy(outputDir = x)
    } text "Path to CSV header"
  }

  def main(av: Array[String]): Unit = {
    val opts = parser.parse(av, Opts()).get
    withSparkContext("NASCAR for SSB") {
      sc =>
        appends(opts, sc)
    }
  }

  def appends(opts: Opts, sc: SparkContext) = {

    val ACXIOM = sc.longAccumulator("OnlyAcxiom")
    val SSB = sc.longAccumulator("OnlySSB")

    val indexes = sc.textFile(s"${opts.indexesFile}").map {
      case line =>
        val row = line.split(",")
        row.last.toInt
    }.collect()

    //Email -> appends
    val appends = sc.textFile(s"${opts.appendsFile}").map {
      line =>
        val parser = new CSVParser()
        val row = parser.parseLine(line)
        row.head -> row.tail.mkString(",")
    }

    //Email -> (id -> required acxiom values)
    val acxiom = sc.textFile(s"${opts.inputFile}", 3072).map {
      line =>
        val parser = new CSVParser()
        val row = parser.parseLine(line)

        val c = Array.fill(29) {""}
        for (i <- 0 until 29) {
          c(i) = row(indexes(i)).replace(",", "")
        }
        row(13) -> (row(0) -> c)
    }

    //Id -> All other fields
    val originalFile = sc.textFile(s"${opts.originalFile}", 3072).map {
      line =>
        val row = line.split('|')
        if (row.length < 15) println(line)
        val c = Array.fill(15) {""}
        for (a <- 0 until 15) {
          c(a) = row(a).replace(",", "")
        }
        c.head -> c.tail.mkString(",")
    }

    //Id -> Acxiom + SR appends
    val d = acxiom.cogroup(appends).flatMap {
      case (key, (axicomFields, appendsFields)) =>
        if (appendsFields.nonEmpty && axicomFields.nonEmpty) {
          axicomFields.flatMap {
            ax =>
              Some(ax._1 -> s"${appendsFields.head},${ax._2.mkString(",")}")
          }
        }
        else if (axicomFields.nonEmpty) {
          axicomFields.flatMap {
            ax =>
              ACXIOM.add(1)
              Some(ax._1 -> s"${Array.fill(65) {""}.mkString(",")},${ax._2.mkString(",")}")
          }
        }
        else None
    }

    //Id , Remaining fields of NASCAR + Acxiom + SR Appends
    originalFile.cogroup(d).flatMap {
      case (key, (original, appendsValues)) =>
        if (original.nonEmpty && appendsValues.nonEmpty) Some(s"$key,${original.head},${appendsValues.head}")
        else if (original.nonEmpty) {
          SSB.add(1)
          Some(s"$key,${original.head},${Array.fill(94) {""}.mkString(",")}")
        }
        else None
    }.saveAsTextFile(s"${opts.outputDir}/appends")

    println(ACXIOM.value)
    println(SSB.value)
  }
}