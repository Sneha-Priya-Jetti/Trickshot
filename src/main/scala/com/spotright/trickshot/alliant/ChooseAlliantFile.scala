package com.spotright.trickshot.alliant

import com.spotright.trickshot.spark.CanSparkContext
import com.spotright.trickshot.util.HelperMethods._
import org.apache.spark.SparkContext

/**
 * Created by sjetti on 7/31/17.
 */
object ChooseAlliantFile extends CanSparkContext {

  def alliant(sc: SparkContext, opts: Opts, input: String): Unit = {
    implicit val isc = sc

    val fileName = input.split("/").last.split("-").drop(3).mkString("-")
    val jobId = input.split("/").last.split("-")(2)
    val positive = sc.longAccumulator("positive")

    val choiceFile = sc.textFile(s"${opts.choiceFile}").map {
      line =>
        val row = line.split(",")
        row.head -> row.last
    }
      .collect()
      .toMap

    val decileValue = choiceFile(jobId).toInt
    val decileRecords = sc.textFile(s"$input").map {
      record =>
        val row = record.split(",")
        val score = row(4).toDouble
        val orgScore = Math.round(score)
        if (orgScore > 0) positive.add(1)
        row(4).toDouble -> s"${row(0)},${row(1)}"
    }
      .collect()

    val splitValue = (positive.value / 10).toInt

    val scoreFileHeader = Array("PLID", "emd5")
    val scoreFile = decileRecords
      .sortBy({ case (score, record) => score })
      .reverse
      .grouped(splitValue)
      .take(decileValue)
      .flatten
      .map {
      case (score, rec) =>
        rec
    }
      .toSeq


    println(s"positive count:: ${positive.value}")
    println(s"DEcile Records count:: $fileName, ${scoreFile.length}")

    writeCSV(scoreFileHeader, scoreFile, s"${opts.output}/$fileName")
  }

  case class Opts(inputFile: String = "",
                  choiceFile: String = "",
                  output: String = "")

  val parser = new scopt.OptionParser[Opts](this.getClass.getSimpleName.stripSuffix("$")) {
    val defaults = Opts()

    note("Arguments:")

    arg[String]("inputfile") action { (x, c) =>
      c.copy(inputFile = x)
    } text "Path to the input Alliant file."

    arg[String]("choicefile") action { (x, c) =>
      c.copy(choiceFile = x)
    } text "Path to the Alliant Choice file."

    arg[String]("output") action { (x, c) =>
      c.copy(output = x)
    } text "Path to the output file."
  }


  def main(args: Array[String]): Unit = {
    val opts = parser.parse(args, Opts()).get
    withSparkContext(this.getClass.getSimpleName.stripSuffix("$")) {
      sc =>
        val files = new java.io.File(s"${opts.inputFile}").listFiles
        files.foreach {
          file =>
            val input = file.toString
            alliant(sc, opts, input)
        }
    }
  }

}
