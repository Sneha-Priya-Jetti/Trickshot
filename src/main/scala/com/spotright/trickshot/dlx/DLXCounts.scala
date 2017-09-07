package com.spotright.trickshot.dlx

import com.spotright.trickshot.spark.CanSparkContext
import org.apache.spark.SparkContext

/**
 * Created by sjetti on 2/24/17.
 */
object DLXCounts extends CanSparkContext {

  case class Opts(header: String = "", inputFile: String = "", outputFile: String = "")

  val parser = new scopt.OptionParser[Opts]("Conflicting Records") {

    arg[String]("<header>") action { (x, c) =>
      c.copy(header = x)
    } text "Path to CSV header"

    arg[String]("<inputfile>") action { (x, c) =>
      c.copy(inputFile = x)
    } text "Path to Input File"

    arg[String]("<outputfile>") action { (x, c) =>
      c.copy(outputFile = x)
    } text "Path to Output File"

  }

  def main(args: Array[String]): Unit = {
    val opts = parser.parse(args, Opts()).get
    withSparkContext("Filter Conflicting Records") {
      sc =>
        worker(opts, sc)
    }
  }

  def worker(opts: Opts, sc: SparkContext): Unit = {
    implicit val isc = sc

    val header = scala.io.Source.fromFile(opts.header).getLines().next().split(",")

    sc.textFile(s"${opts.inputFile}", 3072).map {
      line =>
        val row = line.split(",")
        val dlx = header.zip(row).toMap
        var counters = Seq[String]()

        if (dlx("k_gender") == "F" && dlx("k_hh_composition") == "J") counters :+= s"code1,$line"
        else if (dlx("k_gender") == "F" && dlx("k_hh_composition") == "I") counters :+= s"code2,$line"
        else if (dlx("k_gender") == "M" && dlx("k_hh_composition") == "L") counters :+= s"code3,$line"
        else if (dlx("k_gender") == "M" && dlx("k_hh_composition") == "K") counters :+= s"code4,$line"
        else if (dlx("k_hh_composition") == "B" && dlx("k_children_present") == "Y") counters :+= s"code5,$line"
        else if (dlx("k_hh_composition") == "D" && dlx("k_children_present") == "Y") counters :+= s"code6,$line"
        else if (dlx("k_hh_composition") == "F" && dlx("k_children_present") == "Y") counters :+= s"code7,$line"
        else if (dlx("k_hh_composition") == "H" && dlx("k_children_present") == "Y") counters :+= s"code8,$line"
        else if (dlx("k_hh_composition") == "J" && dlx("k_children_present") == "Y") counters :+= s"code9,$line"
        else if (dlx("k_hh_composition") == "L" && dlx("k_children_present") == "Y") counters :+= s"code10,$line"
        else if (dlx("k_children_present") == "" && dlx("k_children_0_2") == "Y") counters :+= s"code11,$line"
        else if (dlx("k_children_present") == "" && dlx("k_children_3_5") == "Y") counters :+= s"code12,$line"
        else if (dlx("k_children_present") == "" && dlx("k_children_6_10") == "Y") counters :+= s"code13,$line"
        else if (dlx("k_children_present") == "" && dlx("k_children_11_15") == "Y") counters :+= s"code14,$line"
        else if (dlx("k_children_present") == "" && dlx("k_children_16_18") == "Y") counters :+= s"code15,$line"
        else counters :+= s"perfect,$line"
        counters
    }
      .flatMap(identity)
      .coalesce(60)
      .saveAsTextFile(s"${opts.outputFile}/DLXCounts")
  }

}
