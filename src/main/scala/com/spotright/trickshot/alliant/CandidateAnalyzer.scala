package com.spotright.trickshot.alliant

import com.spotright.trickshot.spark.{CanSparkContext, HistogramAcc}
import com.spotright.trickshot.util.Config
import com.spotright.trickshot.util.HelperMethods._

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object CandidateAnalyzer extends CanSparkContext {

  case class Opts(inputFile: String = "", outputDir: String = "")

  val parser = new scopt.OptionParser[Opts]("AlliantCandidateAnalyzer") {

    arg[String]("<inputfile>") action { (x, c) =>
      c.copy(inputFile = x)
    } text "Input Scored File"

    arg[String]("<outputdir>") action { (x, c) =>
      c.copy(outputDir = x)
    } text "Path to CSV header"
  }

  def main(av: Array[String]): Unit = {
    val opts = parser.parse(av, Opts()).get
    withSparkContext("AlliantCandidateAnalyzer") {
      sc =>
        val files = new java.io.File(s"${opts.inputFile}").listFiles
        files.foreach {
          file =>
            val input = file.toString
            sampler(input, opts, sc)
        }
    }
  }

  def sampler(input: String, opts: Opts, sc: SparkContext): Unit = {
    implicit val isc = sc

    val buckets = 0 :: (100000 to 500000 by 100000).toList
    val scoreHist = new HistogramAcc(HistogramAcc.createBuckets(buckets, "scores"))

    val millionsBuckets = 0 :: (1000000 to 3000000 by 500000).toList
    val millionsScoreHist = new HistogramAcc(HistogramAcc.createBuckets(millionsBuckets, "scores"))

    val positive = sc.longAccumulator("positive")
    val negative = sc.longAccumulator("negative")
    val zero = sc.longAccumulator("zero")
    val stdMeanCount = sc.longAccumulator("StandardDeviation")

    val five = sc.longAccumulator("five")
    val ten = sc.longAccumulator("ten")

    val fileName = input.split("/").last.split("-").drop(3).mkString("-")
    val mapping = sc.textFile(s"${Config.hdfs}/user/nutch/Alliant/ScorablePopulation/latest/mappings.csv/*").map {
      line =>
        val row = line.split(",")
        row(0) -> row(3)
    }
      .persist(StorageLevel.DISK_ONLY)

    val scores = sc.textFile(s"$input", 8192).map {
      record =>
        record.split(",")(4).toDouble
    }

    val negBuckets = HistogramAcc.createNegBuckets((1 to Math.abs(scores.min).toInt by 5000).toList.reverse.map(x => x * -1), "scores")
    val zeroBucket = HistogramAcc.createZeroBucket(List(0), "scores")
    val posBuckets = HistogramAcc.createPosBuckets((1 to scores.max.toInt by 5000).toList, "scores")

    val distHist = new HistogramAcc(HistogramAcc.combineBuckets(HistogramAcc.combineBuckets(negBuckets, zeroBucket), posBuckets))

    val count = scores.count()
    val mean = scores.sum / count
    val devs = scores.map(score => (score - mean) * (score - mean))
    val stddev = Math.sqrt(devs.sum / (count - 1))
    val stdMean = stddev + mean

    /**
     * A sampling of the scored records.
     * round the score to the hundreds position (ie. 387 rounds to 400)
     * and arbitrarily chose 3 within that range
     * keep the true score in the sample file
     * Additionally, in order to manually assess the records TWID is retrieved from the Wiland Population file (mapping)
     *
     */

    val sampleHeader = Array("PLID", "Twitter_Id", "emd5", "date1", "date2", "score", "rounded_score", "Twitter URL")
    val sampleScoring = sc.textFile(s"$input").map {
      record =>
        val row = record.split(",")
        val score = row(4).toDouble
        val orgScore = Math.round(score)
        val newScore = Math.round(orgScore / 100) * 100
        if (orgScore > 0) positive.add(1)
        else if (orgScore < 0) negative.add(1)
        else if (orgScore == 0) zero.add(1)
        else println(orgScore)
        if (score > stdMean) {
          stdMeanCount.add(1)
        }
        if (orgScore <= -5001 && orgScore >= -10000) five.add(1)
        if (orgScore <= -10001 && orgScore >= -15000) ten.add(1)
        scoreHist += orgScore.toInt
        millionsScoreHist += orgScore.toInt
        distHist += orgScore.toInt
        newScore -> (row :+ newScore.toString).mkString(",")
    }
      .groupByKey()
      .map {
      case (score, group) =>
        score -> group.take(3)
    }
      .flatMapValues(identity)
      .values
      .map {
      scoredRecord =>
        val d = scoredRecord.split(",")
        (d.head, d.tail)
    }
      .leftOuterJoin(
        mapping.map {
          case (plid, twid) =>
            plid -> twid
        }
      )
      .map {
      case (plid, rest) =>
        s"$plid,${rest._2.get},${rest._1.mkString(",")},https://twitter.com/intent/user?user_id=${rest._2.get}"
    }

    writeCSV(sampleHeader, sampleScoring.collect(), s"${opts.outputDir}/Sample_Scoring-$fileName")

    val histogramHeader = Array("Score From", "Score To", "Count")
    val histogram = distHist.histMap.map {
      case (_, acc) =>
        val title = acc.name.get.replace("hist: scores - [", "").replace(")", "")
        s"$title,${acc.value}"
    }

    writeCSV(histogramHeader, histogram, s"${opts.outputDir}/Score_Histogram-$fileName")

    val scoreFileHeader = Array("PLID", "Twitter_Id", "emd5", "date1", "date2", "score", "rounded_score", "Twitter URL")
    val scoreFile = sc.textFile(s"$input").flatMap {
      record =>
        val row = record.split(",")
        val score = row(4).toDouble
        if (score > stdMean) Some(row.mkString(","))
        else None
    }

    writeCSV(scoreFileHeader, scoreFile.collect(), s"${opts.outputDir}/ScoreFile-$fileName")

    /**
     * A score distribution.
     * round scores to 10 thousands (235,876 rounds to 240,000)
     *
     */

    val distributionHeader = Array("Score", "Count")
    val distribution = sc.textFile(s"$input", 8192).map {
      record =>
        val score = record.split(",")(4).toDouble
        Math.round(score / 1000) * 1000 -> record
    }
      .groupByKey()
      .map {
      case (score, group) =>
        s"$score,${group.size.toString}"
    }

    writeCSV(distributionHeader, distribution.collect(), s"${opts.outputDir}/Score_Distribution_1000-$fileName")

    /**
     * Decile boundaries
     */

    val splitValue = (positive.value / 10).toInt
    val decileHeader = Array("Max of Decile", "Min of Decile")

    val decilevalues = sc.textFile(s"$input").map {
      record =>
        val row = record.split(",")
        row(4).toDouble
    }
      .sortBy(score => score, ascending = false)
      .collect()
      .grouped(splitValue)
      .map {
      case d =>
        s"${d.max},${d.min}"
    }
      .toSeq

    writeCSV(decileHeader, decilevalues, s"${opts.outputDir}/DecileDistribution-$fileName")

    val pos = positive.value.toDouble
    val neg = negative.value.toDouble
    val zer = zero.value.toDouble
    val total = pos + neg + zer

    var analysis = Array.empty[String]

    analysis = analysis ++ Array(s"Positive, ${pos.toString}, ${(pos / total).toString}")
    analysis = analysis ++ Array(s"Zero, ${zer.toString}, ${(zer / total).toString}")
    analysis = analysis ++ Array(s"Negative,${neg.toString},${(neg / total).toString}")
    analysis = analysis ++ Array(s"Total,${total.toString}")
    analysis = analysis ++ Array(s"Mean,${mean.toString}")
    analysis = analysis ++ Array(s"Standard Deviation,${stddev.toString}")
    analysis = analysis ++ Array(s"Standard Deviation + Mean,${stdMean.toString}")
    analysis = analysis ++ Array(s"Standard Deviation + Mean Count,${stdMeanCount.count}")

    analysis = analysis ++ scoreHist.histMap.map {
      case (_, acc) =>
        val title = acc.name.get.replace("hist: scores - [", "").replace(")", "")
        s"$title,${acc.value}"
    }

    analysis = analysis ++ millionsScoreHist.histMap.map {
      case (_, acc) =>
        val title = acc.name.get.replace("hist: scores - [", "").replace(")", "")
        s"$title,${acc.value}"
    }

    writeCSV(Array.empty[String], analysis, s"${opts.outputDir}/Analysis-$fileName")

    println(s"$fileName is Done")
  }
}