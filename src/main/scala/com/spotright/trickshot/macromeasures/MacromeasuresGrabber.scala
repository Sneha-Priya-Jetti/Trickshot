package com.spotright.trickshot.macromeasures

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import com.spotright.common.util.LogHelper
import com.spotright.trickshot.lib.MacromeasuresDAO
import com.spotright.trickshot.util.Config
import scopt._

import scala.io.Source

object MacromeasuresGrabber extends LogHelper {

  def worker(opts: Opts): Unit = {

    logger.info(s"Using input file: ${opts.input}")

    val grouping = Config.mmBatchSize

    val apiKey = opts.runMode match {
      case 'prod => Config.mmProdApiKey
      case 'test => Config.mmTestApiKey
      case _ => sys.error(s"Unchecked runMode: ${opts.runMode}")
    }

    // Print info about our rate limit as an fyi
    printRateLimit(apiKey)

    val idGroups = Source.fromFile(opts.input)
      .getLines
      .grouped(grouping)
      .zipWithIndex

    idGroups
      .foreach {
        case (ids, indx) =>
          logger.info(s"ID list: ${ids.take(10).mkString(",")},...")

          val mResponse = MacromeasuresDAO.getInstagramDataForIds(ids, apiKey)

          val jsResp = mResponse.map(_.compactPrint)

          jsResp.foreach { js => Files.write(Paths.get(f"${opts.output}/batch-$indx%09d"), s"$js\n".getBytes(UTF_8)) }
      }

    printRateLimit(apiKey)
  }

  def printRateLimit(apiKey: String): Unit = MacromeasuresDAO.getRateLimit(apiKey) match {
    case Some(rl) =>
      logger.info("Macromeasures API Rate Limit info:")
      logger.info(f"  ├── Lifetime limit:     ${rl.lifetime_limit}%,d")
      logger.info(f"  ├── Lifetime remaining: ${rl.lifetime_remaining}%,d")
      logger.info(f"  ├── Period limit:       ${rl.period_limit}%,d")
      logger.info(f"  ├── Period remaining:   ${rl.period_remaining}%,d")
      logger.info(f"  └── Period reset:       ${rl.period_reset}%,d seconds")

    case _ => logger.error("Unable to reach Macromeasures API rate limit endpoint.")
  }

  case class Opts(input: String = "",
                  output: String = "",
                  runMode: Symbol = 'test)

  val parser = new OptionParser[Opts](this.getClass.getSimpleName.stripSuffix("$")) {

    note("Arguments:")

    arg[String]("input") required() action {
      (x, c) => c.copy(input = x)
    } text "Path to input file containing Instagram IDs."

    arg[String]("output") required() action {
      (x, c) => c.copy(output = x)
    } text "Path to output file in json format."

    note("Options:")

    opt[String]("runMode") required() action {
      (x, c) => c.copy(runMode = Symbol(x))
    } text "Specifies which Macromeasures api to query. Must be \"prod\" or \"test\"."
  }

  def main(args: Array[String]): Unit = {
    parser.parse (args, Opts () ) match {
      case Some (opts) => worker(opts)
      case _ => // args are bad, error message will have been displayed
    }
  }
}