package com.spotright.trickshot.cassdata

import java.time._

import com.datastax.spark.connector.cql.CassandraConnector
import com.spotright.common.time._
import com.spotright.common.util.InputTidy._
import com.spotright.search.client.SpotSolr
import com.spotright.trickshot.lib.SolrDao._
import com.spotright.trickshot.spark.CanSparkContext
import com.spotright.trickshot.util.Config
import com.spotright.xcass._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scopt.OptionParser

import scala.collection.JavaConverters._

/**
 * Created by sjetti on 1/13/17.
 */
object TwHandlesMatchRate extends CanSparkContext {

  case class Opts(handlesList: String = "", outputFile: String = "")

  val parser = new OptionParser[Opts]("SolrCassGraphDrift") {

    arg[String]("<handlesList>") action { (x, c) =>
      c.copy(handlesList = x)
    } text "Input handles File"

    arg[String]("<OutputFile>") action { (x, c) =>
      c.copy(outputFile = x)
    } text "Output File Location"
  }

  def main(args: Array[String]) {
    val opts = parser.parse(args, Opts()).get
    withSparkContext("TweetHandlesLookUp") {
      sc =>
        worker(sc, opts)
    }
  }

  def worker(sc: SparkContext, opts: Opts) = {
    implicit val isc = sc

    val handles = sc.textFile(opts.handlesList).map {
      handle => normalizeTwitterHandle(handle).getOrElse(handle.replace("@", ""))
    }

    handlesFromTwitter(handles, opts, sc)
  }

  /**
   * To find handles which are able to make into Solr Graph from the given set of handles
   *
   * @param handles : Input Set of Handles
   * @param opts
   * @param sc
   */

  def handlesFromTwitter(handles: RDD[String], opts: Opts, sc: SparkContext) = {

    val cassConn = CassandraConnector(sc.getConf)

    val lastYearMillis = Instant.now.minusYears(1).toEpochMilli
    handles.foreachPartition {
      iter =>
        val trueId = new java.io.FileWriter(s"${opts.outputFile}/Exist.csv", true)
        val falseId = new java.io.FileWriter(s"${opts.outputFile}/NotFound.csv", true)

        cassConn.withSessionDo { implicit session =>
        iter.foreach {
          handle =>

            val following = SpotCass.twitter.selectRow(R2(handle, "following_count")).getMany().map {
              col =>
                Instant.ofEpochMilli(col.timestamp) -> col.column2.toLong
            }
              .sortBy(_._1)

              val followingCount = if(following.nonEmpty) following.head._2
            else 0

            val tags = SpotCass.twitter.selectRow(R2(handle, "tags")).getMany().map {
              col =>
                col.column2
            }
              .toSet

            val recentTweeter = SpotCass.twitter.selectRow(R2(handle, "tweets")).getMany().map {
              col =>
                col.tsInstant.toEpochMilli > lastYearMillis
            }
              .size >= 2

            val isConsumer = followingCount >= 1

            if (isConsumer || recentTweeter || tags("sr-linked") || tags("wiland")) {
              trueId.write(s"$handle\n")
            }
            else falseId.write(s"$handle\n")
        }
      }
        trueId.close()
        falseId.close()
    }

  }

  /**
   * To find handles which made into Solr Graph from the given set of handles
   * @param handles
   * @return
   */

  def handleFromGraph(handles: List[String]): Map[String, String] = {

    val sq2 = SpotSolr.solrQuery("graph-2017-01", "/select")
      .setBatchQuery("handle", handles)
      .setUniqSort
      .setFields("handle", "id")
      .setRows(handles.length)

    SpotSolr.withServer { csc => SpotSolr.deepQueryIterator(sq2, csc) }.flatMap {
      qr =>
        qr.getResults.asScala.map { doc =>
          doc.getFieldValue("handle").asInstanceOf[String] -> doc.getFieldValue("id").asInstanceOf[String]
        }
    }.toMap
  }

  /**
   * Get twitter Id with handle from Full Contacts
   * @param twid
   * @return
   */
  def handleFromFc(twid: String): Set[String] = {
    val sq = SpotSolr.solrQuery(Config.fullcontactCollection, "/select")
      .setQuery(s"social_id_twitter:$twid")
      .setFields("social_username_twitter")

    SpotSolr.withServer(_.query(sq)).getResults.asScala.flatMap {
      doc =>
        doc.safelyGetFieldValues("social_username_twitter")
    }.toSet
  }
}
