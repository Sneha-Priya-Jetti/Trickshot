package com.spotright.trickshot.lib

import java.time._

import com.spotright.common.time._
import com.spotright.common.util.LogHelper
import com.spotright.models.solr.{SolrDateOps, SpotSolrInputDocument}
import com.spotright.search.bigquery.QueryBuilder._
import com.spotright.search.client.SpotSolr
import com.spotright.search.termbased.{TermBasedParser => TBP, UnitTermBasedParser => UTBP}
import com.spotright.trickshot.util.Config
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.request.QueryRequest
import org.apache.solr.common.SolrDocument
import org.apache.solr.common.cloud.ZkCoreNodeProps
import org.apache.solr.common.params.CollectionParams.CollectionAction
import org.apache.solr.common.params.{CoreAdminParams, ModifiableSolrParams}

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Encapsulate some solr data access patterns
  */

object SolrDao extends SolrDateOps with LogHelper {

  // http://yonik.com/solr-terms-query/
  def termsQueryStr(field: String, queryValues: TraversableOnce[String], cache: Boolean = true): String = {
    s"{!terms cache=$cache f=$field}${queryValues.mkString(",")}"
  }

  // deepQueryIterator pattern helper
  def dqi[A](sq: SolrQuery, f: SolrDocument => A): Iterator[A] = {

    sq.setUniqSort

    SpotSolr.withServer { csc => SpotSolr.deepQueryIterator(sq, csc) }.flatMap {
      _.getResults.asScala.map {
        doc => f(doc)
      }
    }
  }

  // Get all active collections
  def listCollections: List[String] = {
    // easy way, but non testable
    //    val cols = scala.io.Source.fromURL("http://sr201:8983/solr/admin/collections?action=LIST&wt=json").getLines().next.parseJson \ "collections"
    //    cols.convertTo[List[String]]

    // copied from SpotMiniSolrCloudCluster.scala
    val params: ModifiableSolrParams = new ModifiableSolrParams
    params.set(CoreAdminParams.ACTION, CollectionAction.LIST.name)
    val request: QueryRequest = new QueryRequest(params)
    request.setPath("/admin/collections")

    SpotSolr.createCloudSolrClient(null).request(request)
      .get("collections").asInstanceOf[java.util.ArrayList[String]].asScala.toList
  }

  // provide a base and get the latest actual collection
  // id fullcontact -> fullcontact-2016-04
  def getLatestCollection(collBase: String): String = {
    listCollections.filter(_.contains(collBase)).sorted.last
  }

  /**
    * Given a content collection alias, ie content-4months we want to know what is the
    * earliest collection so we can filter the tweets coming from cassandra properly.
    * Note that  DateTime.now.minusMonths(4) is incorrect because the content aliases can
    * contain anywhere from 4-5 months of actual data.
    *
    * In the devolopment mode case there are no aliases so we want to just cast back to
    * the farthest range.
    */
  val contentFmt = DefaultedDateTimeFormatter.ofPattern("yyyy-MM")

  def getAliasBoundary(alias: String, baseName: String = "tweets"): OffsetDateTime = {
    if (Config.isRunModeDevelopment) {
      OffsetDateTime.now.minusMonths(13) // map test cases back 13 months
    } else {
      //   tweets-            2016-05
      val dtStr = collectionsInAlias(alias).min.substring(baseName.length + 1, baseName.length + 1 + 7)
      OffsetDateTime.parse(dtStr, contentFmt)
    }
  }

  // get all the collections in an alias, safe to use on non-aliased collections or testing: alias == List(alias)
  // command line testing
  // curl -s -d "action=CLUSTERSTATUS&wt=json" http://sr201:8983/solr/admin/collections | jq '.cluster.aliases.fullcontact'
  def collectionsInAlias(coll: String): List[String] = {
    val csc = SpotSolr.createCloudSolrClient(coll)
    // force connection to register the zk state reader
    csc.connect()

    val colls = Try {
      // too many possible nulls to track here
      csc.getZkStateReader.getAliases.getCollectionAliasMap.asScala(coll)
        .split(",").toList
    }
      .getOrElse(List(coll))

    csc.close()
    colls
  }

  // get shards of a particular collection, id
  // List(http://sr214:8983/solr/graph-2016-03_shard10_replica2/, http://sr205:8983/solr/graph-2016-03_shard11_replica1/, ...)
  def getShards(collection: String): List[String] = {
    val csc = SpotSolr.createCloudSolrClient(collection)
    // force connection to register the zk state reader
    csc.connect()
    val clusterState = csc.getZkStateReader.getClusterState
    val liveNodes = clusterState.getLiveNodes

    val shards = collectionsInAlias(collection).flatMap {
      coll =>
        clusterState.getActiveSlices(coll).asScala.flatMap {
          slice =>
            val replicas = slice.getReplicas.asScala.flatMap {
              replica =>
                val replicaCoreProps = new ZkCoreNodeProps(replica)
                if (liveNodes.contains(replicaCoreProps.getNodeName)) Some(replicaCoreProps.getCoreUrl) else None
            }

            val numReplicas = replicas.size

            if (numReplicas == 0) {
              logger.error(s"Shard ${slice.getName} in collection $coll has no active replicas!  Skipping...")
              None
            } // pick a random replica
            else Some(replicas.toList(scala.util.Random.nextInt(numReplicas)))
        }
    }

    csc.close()
    shards
  }

  // Helper to get at the neat solr dsl parsing
  def parseSolrDsl(in: String): String = {

    TBP.parseAll(TBP.query, in) match {
      case TBP.Success(query, _) => query.toString()
      case TBP.Failure(msg, _) => sys.error(msg)
      case TBP.Error(msg, _) => sys.error(msg)
    }
  }

  // Helper to get at the neat solr dsl parsing and return individual tokens
  // NOTE: the ._1 value will be exactly the same as parseSolrDsl above
  def parseSolrDslWithLeaves(in: String): (String, List[String]) = {
    TBP.optimizedParseWithLeaves(in) match {
      case Some(xs) => (xs.head, xs.tail)
      case None => sys.error(s"Error parsing: $in")
    }
  }

  // Helper to get at the neat solr dsl parsing and return individual tokens w/out handle->twid conversion
  // NOTE: the ._1 value will be exactly the same as parseSolrDsl above
  def parseSolrDslWithTerms(in: String): (String, List[String]) = {
    UTBP.optimizedParseWithLeaves(in) match {
      case Some(xs) => (xs.head, xs.tail.flatMap(dslFieldToTerm))
      case None => sys.error(s"Error parsing: $in")
    }
  }

  // Helper to turn a parsed Solr DSL field back into the original term
  // hashtags:spotright => #spotright
  def dslFieldToTerm(in: String): Option[String] = {
    if (in.startsWith("mentions_twids:"))
      Some(in.stripPrefix("mentions_twids:"))
    else if (in.startsWith("hashtags:"))
      Some(s"#${in.stripPrefix("hashtags:")}")
    else if (in.startsWith("text:"))
      Some(in.stripPrefix("text:\"").stripSuffix("\""))
    else None
  }

  def tagGraph(twid: String, handle: String, tag: String): SpotSolrInputDocument = {
    val graphDoc = new SpotSolrInputDocument
    graphDoc.setField("id", twid)
    graphDoc.setField("handle", handle)
    graphDoc.addField("tags", Map("add" -> tag).asJava)

    graphDoc
  }

  implicit class PimpedSolrQuery(sq: SolrQuery) {

    // to retrieve a batch of docs
    def setBatchQuery(field: String, queryValues: TraversableOnce[String], cache: Boolean = true): SolrQuery = {
      sq.setQuery("*:*") // uses MatchAllDocQuery that avoids scoring
        .setFilterQueries(termsQueryStr(field, queryValues, cache)) // use terms parser in the filter query based on yonik.com/solr-term-query
    }

    // unique sorters for use, for example, in DQI cursoring
    // ToDo: can we autmatically get the uniqueKey field name from the sq itself? ie sq.get("uniqueKey")
    def setUniqSort = {
      val field = sq.get("collection") match {
        case c if c.startsWith("content") => "id"
        case d if d.startsWith("demog_idr") => "id"
        case d if d.startsWith("demog_dlx") => "id"
        case d if d.startsWith("demog_exp") => "id"
        case f if f.startsWith("fullcontact") => "emailMd5"
        case g if g.startsWith("graph") => "id"
        case i if i.startsWith("instagram") => "instagramId"
        case p if p.startsWith("profiles") => "guid"
        case t if t.startsWith("tweets") => "id"
        case x => sys.error(s"I don't know the unique field for collection:: $x\n$sq")
      }

      sq.setSort(field, SolrQuery.ORDER.desc)
    }

    def setShards(shards: String*) = {
      sq.set("shards", shards.mkString(","))
      sq
    }

    // graph
    def filterTag(tag: String) = sq.addFilterQuery(Must("tags", tag).toString)

    def linkedOnly = filterTag("sr-linked")

    def consumer = sq.addFilterQuery(Must("followingCount", "[1 TO *]").toString)

    def instaConsumer = sq.addFilterQuery(Must("interests", "*").toString)

    // tweets
    def isConsumer = sq.addFilterQuery(Must("is_cons", "true").toString())

    def addContentWindowDays(days: Int) = sq.addFilterQuery(Must("created_at", s"[NOW-${days}DAYS TO NOW]").toString())

    def addContentWindow(start: Instant, end: Instant) = sq.addFilterQuery(Must("created_at", s"[${dt2SolrDaily(start)} TO ${dt2Solr(end)}]").toString())

    def retweetsOnly = sq.addFilterQuery(Must("rt", "true").toString)
  }

  /**
    * Ease of working with docs from solr
    */
  implicit class PimpedSolrDoc(doc: SolrDocument) {
    def safelyGetFieldValues(field: String): Iterable[String] = Try {
      doc.getFieldValues(field).asScala.map(_.toString)
    }
      .getOrElse(Iterable.empty[String])
  }

  /**
    * Sometimes you want to grab things from the input doc itself.  These methods will closely
    * mirror PimpedSolrDoc methods
    */
  implicit class PimpedSpotSolrInputDoc(doc: SpotSolrInputDocument) {
    def safelyGetFieldValues(field: String): Iterable[String] = Try {
      doc.getFieldValues(field).asScala.map(_.toString)
    }
      .getOrElse(Iterable.empty[String])
  }

}
