package com.spotright.trickshot.alliant

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import com.spotright.common.util.LogHelper
import com.spotright.trickshot.spark.CanSparkContext
import com.spotright.trickshot.util.{Config, HelperMethods}

object AlliantWilandOverlap extends CanSparkContext with LogHelper {

  def alliant(implicit spark: SparkSession, opts: Opts): Unit = {

    import spark.implicits._

    val alliantRows = spark
      .read
      .schema(
        new StructType()
          .add("PERSON_LINK_KEY", StringType)
          .add("emailMd5", StringType)
          .add("twitterId", StringType)
          .add("matchLevel", StringType)
      )
      .option("delimiter", ",")
      .option("mode", "FAILFAST")
      .csv(s"${Config.hdfs}/user/nutch/Alliant/ScorablePopulation/20170630_20170714/mappings.csv/*") // Drop records with PERSON_LINK_KEY "(null)".
      .persist(StorageLevel.DISK_ONLY)

    val emailMD5s = alliantRows
      .select(
        'emailMd5
      ).distinct()

    val twitterIds = alliantRows
      .select(
        'twitterId
      ).distinct()

    val matches = alliantRows
      .select(
        'emailMd5,
        'twitterId
      ).distinct()

    logger.info(f"Alliant emailMd5s count: ${emailMD5s.count()}%,d")
    logger.info(f"Alliant twitterIds count: ${twitterIds.count()}%,d")
    logger.info(f"Alliant emailMd5s and twitterId pair counts: ${matches.count()}%,d")

    val wilandRows = spark
      .read
      .schema(
        new StructType()
          .add("WilandId", StringType)
          .add("emd5", StringType)
          .add("matchLevel", StringType)
          .add("twitterId", StringType)
      )
      .option("delimiter", ",")
      .option("mode", "FAILFAST")
      .csv(s"${Config.hdfs}/user/nutch/Wiland/ScorablePopulation/latest/mappings.csv/*") // Drop records with PERSON_LINK_KEY "(null)".
      .persist(StorageLevel.DISK_ONLY)


    val emd5s = wilandRows
      .select(
        'emd5
      ).distinct()

    val twids = wilandRows
      .select(
        'twitterId
      ).distinct()

    wilandRows
      .groupBy('matchLevel)
      .count()
      .show(truncate = false)

    val wilandmatches = wilandRows
      .select(
        'emd5,
        'twitterId
      ).distinct()

    logger.info(f"wiland emailMd5s count: ${emd5s.count()}%,d")
    logger.info(f"wiland twitterIds count: ${twids.count()}%,d")
    logger.info(f"wiland emailMd5s and twitterId pair counts: ${wilandmatches.count()}%,d")

    val commonEmd5s = emailMD5s.join(emd5s, emailMD5s("emailMd5") === emd5s("emd5")).distinct()
    val commonTwids = twitterIds.join(twids, "twitterId").distinct()
    val commonEmd5Twids = matches.join(wilandmatches, (alliantRows("emailMd5") === wilandRows("emd5")) && (alliantRows("twitterId") === wilandRows("twitterId"))).distinct()

    logger.info(f"common emailMd5s count: ${commonEmd5s.count()}%,d")
    logger.info(f"common twitterIds count: ${commonTwids.count()}%,d")
    logger.info(f"common emailMd5s and twitterId pair counts: ${commonEmd5Twids.count()}%,d")

    val allMatches = alliantRows
      .join(wilandRows, (alliantRows("emailMd5") === wilandRows("emd5")) && (alliantRows("twitterId") === wilandRows("twitterId")))
      .select('PERSON_LINK_KEY, 'WilandId, 'emailMD5)
      .distinct()
      .groupBy('PERSON_LINK_KEY)
      .agg(
        min(
          struct(
            'WilandId,
            'emailMD5
          )
        ) as "bestMatch"
      )
      .select('PERSON_LINK_KEY, $"bestMatch.WilandId" as "WilandId", $"bestMatch.emailMD5")
      .distinct()
      .persist(StorageLevel.DISK_ONLY)

    logger.info(f"Common between Alliant and Wiland: ${allMatches.count()}%,d")

    //allMatches.write.csv(s"${opts.output}/PLID-WilandID.csv")

    val files = new java.io.File(s"${opts.inputFile}").listFiles

    files.foreach {
      file =>
        val input = file.toString
        val fileName = input.split("/").last
        val scoredRows = readScoredFile(input)

        val alliantScoreRecords = allMatches
          .join(scoredRows, "WilandId")
          .select('PERSON_LINK_KEY, 'emailMd5, 'latest_action, 'latest_scorable, 'score)
          .distinct()

        alliantScoreRecords.write.csv(s"${opts.output}/Allinat-$fileName")
        logger.info(s"Done scoring $fileName")
    }

    /*(alliantRows.
      groupBy("emailMd5", "twitterId")
      .count()
      .select('emailMd5 as "emd5", 'twitterId as "twid", 'count, 'count > 10 as "flag") as "distribution")
      .join(alliantRows, (alliantRows("emailMd5") === $"distribution.emd5") && (alliantRows("twitterId") === $"distribution.twid") && $"distribution.flag" === true)
    .select('PERSON_LINK_KEY, 'emailMd5, 'twitterId, 'count)

    //alliantRows.groupBy("emailMd5", "twitterId").count().withColumnRenamed("count", "distCount").groupBy("distCount").count().write.csv(s"${opts.output}/Person_link_key_dist.csv")*/

  }

  def readScoredFile(path: String)
                    (implicit spark: SparkSession): DataFrame = {

    val scoredRows = spark
      .read
      .schema(
        new StructType()
          .add("WilandId", StringType)
          .add("emd5", StringType)
          .add("latest_action", StringType)
          .add("latest_scorable", StringType)
          .add("score", StringType)
      )
      .option("mode", "FAILFAST")
      .csv(path)
      .distinct()
      .persist(StorageLevel.DISK_ONLY)

    scoredRows
  }

  case class Opts(inputFile: String = "",
                  output: String = "",
                  fcPath: String = s"${HelperMethods.getLatestFullContactPath}/linked.csv")

  val parser = new scopt.OptionParser[Opts](this.getClass.getSimpleName.stripSuffix("$")) {
    val defaults = Opts()

    note("Arguments:")

    arg[String]("inputfile") action { (x, c) =>
      c.copy(inputFile = x)
    } text "Path to the input Alliant file."

    arg[String]("output") action { (x, c) =>
      c.copy(output = x)
    } text "Path to the output file."

    opt[String]("fc-file") action { (x, c) =>
      c.copy(fcPath = x)
    } text "Path to the input FC linked.csv file."
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, Opts()) match {
      case Some(opts) => withSparkSession(this.getClass.getSimpleName.stripSuffix("$")) { spark => alliant(spark, opts) }
      case None => // arguments are bad, error message will have been displayed
    }
  }
}