package com.spotright.trickshot.contrib

import com.opencsv.CSVParser
import com.spotright.common.util.{InputTidy, LogHelper, NoneIfEmpty}
import com.spotright.trickshot.spark.CanSparkContext
import com.spotright.trickshot.util.{Config, HelperMethods, Helpers}
import com.spotright.xcass.spark._
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.SparkContext
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.util.{Failure, Success, Try}
import scalaj.http.Http

/**
  * Uploads seeds to Deadpool's MongoDB seeds collection
  * via the Deadpool API. Accepts a csv file of the format:
  *
  * mmInstagramTitle,twitterHandle,mmInstagramId
  *
  * mmInstagramHandle and mmInstagramId can be null, but
  * twitterHandle is required.
  */

object SeedUploader extends CanSparkContext with LogHelper {

  def runJob(implicit sc: SparkContext, opts: Opts): Unit = {

    val cassConn = CassandraConnector(sc.getConf)

    val seeds = sc.textFile(opts.matchedFile)
      .flatMap {
        line =>

          val mTokens = Try {
            val parser = new CSVParser()
            val tokens = parser.parseLine(line)
            require(tokens.length == 3, s"Input line does not adhere to format igName,twHandle,mmId:: $line")
            tokens
          } match {
            case Success(tokens) => Some(tokens)
            case Failure(ex) =>
              logger.warn(Helpers.errorMsgAndStackTrace(ex))
              Option.empty[Array[String]]
          }

          mTokens.flatMap { tokens =>
            val Array(igName, twHandle, igId) = tokens

            InputTidy.normalizeTwitterHandle(twHandle).map { handle =>

              val twitterMeta = cassConn.withSessionDo { implicit session =>
                HelperMethods.getTwitterMetaSafely(handle)
              }

              val mIgName = NoneIfEmpty(igName)
              val mIgId = NoneIfEmpty(igId)

              val tmName = twitterMeta.name.map(StringEscapeUtils.unescapeHtml4)
              val tmBio = twitterMeta.description.map(StringEscapeUtils.unescapeHtml4)

              val atHandle = Some(s"@$handle")
              val name = tmName orElse mIgName orElse atHandle
              val twitterBio = tmBio orElse Option("")

              val ret = Map(
                "name" -> name,
                "twitterHandle" -> atHandle,
                "twitterBio" -> twitterBio,
                "instagramMMTitle" -> mIgName,
                "instagramMMId" -> mIgId
              )

              ret.toJson
            }
          }
      }

    seeds.mapPartitions { iter =>
      iter.map { jsSeed =>
        val seedString = jsSeed.compactPrint

        val url = opts.runMode match {
          case 'prod => Config.deadpoolProdUri
          case 'stage => Config.deadpoolStageUri
          case _ => sys.error(s"Unchecked runMode: ${opts.runMode}")
        }

        val ep = Config.deadpoolSeedsEndpoint

        val req = Http(s"$url/$ep")
          .header("Content-Type", "application/json")
          .postData(seedString)

        Try {
          req.asString.throwError.body
        } match {
          case Success(_) =>
            s"success\t$seedString"
          case Failure(ex) =>
            logger.warn(s"Failure adding seed: $seedString - Error: ${Helpers.errorMsgAndStackTrace(ex)}")
            s"failure\t$seedString"
        }
      }
    }
      .saveAsTextFile(opts.output)
  }

  case class Opts(matchedFile: String = "",
                  output: String = "",
                  runMode: Symbol = 'stage)

  val parser = new scopt.OptionParser[Opts](this.getClass.getSimpleName.stripSuffix("$")) {
    val defaults = Opts()

    note("Arguments:")

    arg[String]("matchedFile") required() action { (x, c) =>
      c.copy(matchedFile = x)
    } text "Path to the input MM interest name, Twitter Handle, MM interest ID file."

    arg[String]("output") required() action { (x, c) =>
      c.copy(output = x)
    } text "Path to output file in TSV format."

    note("Options:")

    opt[String]("runMode") required() action { (x, c) =>
      c.copy(runMode = Symbol(x))
    } text "Specifies which Deadpool to load to. Must be \"prod\" or \"stage\"."
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, Opts()) match {
      case Some(opts) => withSparkContext(this.getClass.getSimpleName.stripSuffix("$")) { sc => runJob(sc, opts) }
      case None => // arguments are bad, error message will have been displayed
    }
  }
}