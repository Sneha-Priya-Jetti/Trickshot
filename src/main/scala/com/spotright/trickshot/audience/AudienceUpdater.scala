package com.spotright.trickshot.audience

import com.spotright.trickshot.lib.SolrDao._
import com.spotright.trickshot.spark.CanSparkContext
import com.spotright.models.solr.Audience
import com.spotright.search.client.SpotSolr
import org.apache.solr.client.solrj.SolrQuery.ORDER
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

/**
  * Created by sjetti on 12/13/16.
  */
object AudienceUpdater extends CanSparkContext {

  val csc = SpotSolr.createCloudSolrClient("audience", "id")

  def loadDir(opts: Opts, sc: SparkContext) = {
    implicit val isc = sc

    val RECORDS = sc.longAccumulator("total_records")

    val audience_ids = sc.textFile(s"${opts.audienceIds}")
      .mapPartitions {
        part =>
          part.map { line =>
            val row = line.split(",")
            row.head -> row.last
          }
      }
      .collect()
      .toMap

    val inputData = sc.textFile(s"${opts.dir}/*", 3072)
      .mapPartitions {
        part =>
          part.map {
            case line =>
              val row = line.split(",")
              row.head -> row.tail.map { aud => audience_ids(aud) }.toList
          }
      }

    var good = 0
    var bad = 0

    inputData.foreachPartition {
      eachPart =>
        val outputData = solrDocs(eachPart.toMap, audience_ids)
        val iter = outputData.flatMap {
          case (id, audience) =>
            val a = Audience.apply(id, audience.distinct)
            Try {
              a.asSolrInputDoc
            }
              .transform[Iterator[SolrInputDocument]](
              v => {
                good += 1
                Success(Iterator.single(v))
              },
              e => {
                val msg = e.getMessage
                logger.error(s"error creating solrDoc: ${a.id} - $msg")
                bad += 1
                Success(Iterator.empty)
              }
            ).get
        }

        iter.foreach {
          doc =>
            RECORDS.add(1)
            SpotSolr.solrAdd(csc, Seq(doc))
            if (iter.isEmpty) csc.close()
        }

        println(good)
        println(bad)
        println(RECORDS.value)
    }
  }

  def solrDocs(ids: Map[String, List[String]], audience_ids: Map[String, String]): Map[String, List[String]] = {

    val sq = SpotSolr.solrQuery("audience", "/select")
      .setBatchQuery("id", ids.keySet)
      .addSort("id", ORDER.asc)

    val solrData = SpotSolr.withServer { csc => SpotSolr.deepQueryIterator(sq, csc) }.flatMap {
      qr =>
        qr.getResults.asScala.map { doc =>
          doc.getFieldValue("id").asInstanceOf[String] ->
            doc.getFieldValues("sr_audience_ids").asScala
              .map { aud => audience_ids(aud.toString) }
              .toList
        }
    }.toMap

    ids.map {
      case (id, audience_id) =>
        solrData.contains(id) match {
          case true => id -> (solrData(id) ++ audience_id)
          case false => id -> audience_id
        }
    }
  }

  case class Opts(dir: String = "", audienceIds: String = "")

  val parser = new scopt.OptionParser[Opts]("AudienceUpdater") {

    arg[String]("<dir>") action { (x, c) =>
      c.copy(dir = x)
    } text "Directory to load"

    arg[String]("<audienceIds>") action { (x, c) =>
      c.copy(audienceIds = x)
    } text "Directory to load"

  }

  def main(av: Array[String]) = {
    val opts = parser.parse(av, Opts()).get
    withSparkContext("AudienceSolrUpdater") {
      sc =>
        loadDir(opts, sc)
    }
  }

}
