package com.spotright.trickshot.util

import com.spotright.common.config.ConfigLoader

/**
  * Created with IntelliJ IDEA.
  * User: dave
  * Date: 9/3/13
  * Time: 10:33 PM
  * To change this template use File | Settings | File Templates.
  */

/**
  * Object Config is a singleton and will not catch refresh semantics
  * for the lifetime of the jvm.  To catch refresh semantics use
  * {{{
  *   new Config
  * }}}
  *
  */
object Config extends Config

class Config extends ConfigLoader("trickshot") {

  // Values from Lift: development, test, staging, production, pilot, profile
  val runMode = getString("runmode")
  val isRunModeProduction = runMode == "production"
  val isRunModeDevelopment = runMode == "development"

  val zkHosts = getStringList("zkHosts").toList

  // If kafkaHosts is empty then EmbeddedKafka will choose random ports on localhost for testing purposes
  val kafkaHosts = getStringList("kafkaHosts").toList
  val kafkaPort = Option(getString("kafkaPort")).getOrElse("9092")

  val defaultZkConnectString = zkHosts.mkString(",") + "/kafka"

  val defaultBrokersString = {
    val hosts = if (kafkaHosts.isEmpty) List("localhost") else kafkaHosts
    hosts.map(_ + ":" + kafkaPort).mkString(",")
  }

  val refreshThreshold = getInt("refreshThreshold")
  val idResolveLimit = getInt("idResolveLimit")

  // spark configs
  val sparkMaster = getString("spark.master")
  val sparkHome = getString("spark.home")
  val sparkTerminateSecs = getInt("spark.terminateSecs")

  val sparkCleanerTtl = getInt("spark.cleaner.ttl")
  // seconds  *** unsafe, not using, instead see o.a.spark.ContextCleaner
  // the time interval (seconds) at which streaming data will be divided into batches
  val sparkDuration = getInt("spark.duration")
  // max messages per second for each streaming reciever
  val sparkStreamingRateLimit = getInt("spark.streaming.rate.limit")

  val kafkaReset = getBoolean("kafka.reset")

  val hdfs = getString("fs.default.name")
  val crawlDfs = getString("crawl.fs.default.name")

  val dbiUrl = getString("dbiUrl")

  val awsBucket = getString("awsBucket")
  val awsAccess = getString("awsAccess")
  val awsSecret = getString("awsSecret")

  val srSftpHost = getString("sftp.sr.host")
  val srSftpPort = getInt("sftp.sr.port")

  val wilandSftpHost = getString("sftp.wiland.host")
  val wilandSftpPort = getInt("sftp.wiland.port")
  val wilandSftpUserId = getString("sftp.wiland.userId")
  val wilandSftpDir = getString("sftp.wiland.directory")

  // number of elements to report in the various IandA fields
  val iandaBigSize = getInt("ianda.bigSize")
  val iandaInterestCatTop = getInt("ianda.interestCatTop")
  val iandaInterestCatSize = getInt("ianda.interestCatSize")
  // term based segment collection
  val tbsCollection = getString("ianda.tbs_collection")
  // default tweets collection
  val tweetsCollection = getString("ianda.tweets_collection")
  val fullcontactCollection = getString("ianda.fullcontact_collection")
  // audiences collection
  val audiencesCollection = getString("ianda.audiences_collection")
  // instagram collection
  val instagramCollection = getString("ianda.instagram_collection")
  // demographics sources
  val defaultDemogSource = getString("ianda.demog_sources.default_source")
  val demogSources = getMap[String]("ianda.demog_sources.collection_map")

  val numPopularUniqueBrands = getInt("ianda.popular_unique_brands")
  // epsilon file of eps_id -> emd5 pairs
  val epsilonPopulationFilter = hdfs + getString("ianda.epsilon_population_filter")
  val experianPopulationFilter = hdfs + getString("ianda.experian_population_filter")

  // URL for the indv_data dictionary endpoint
  val indvDictionaryEndpoint = getString("ianda.indv_dictionary_endpoint")

  // graph collection
  val handleIdMapFile = getString("graph_loader.handle_id_map_file")

  // kraken proxy for fetching data from twitter
  val proxyHost = getString("proxy_host")
  val proxyPort = getInt("proxy_port")
  val krakenNodeCount = getInt("kraken_node_count")

  // mongodb.uri for Wiland ScoringStatus
  val scoringStatusMongodbUri = getString("mongodb.wiland-scoring-status.uri")

  // mongodb.uri for audiences
  val audiencesProdMongodbUri = getString("mongodb.audiences.prod.uri")
  val audiencesStageMongodbUri = getString("mongodb.audiences.stage.uri")

  // configable thread pool params
  val threadPoolType       = getString("thread_pool.type")
  val threadPoolNumThreads = getInt("thread_pool.num_threads")

  // deadpool api for topics/seeds
  val deadpoolProdUri = getString("deadpool.prod.uri")
  val deadpoolStageUri = getString("deadpool.stage.uri")
  val deadpoolSeedsEndpoint = getString("deadpool.endpoint.seeds")

  // macromeasures api for sourcing instagram data
  val mmApiUri = getString("macromeasures.api.uri")
  val mmUsersEndpoint = getString("macromeasures.endpoint.users")
  val mmLimitsEndpoint = getString("macromeasures.endpoint.limits")

  val mmProdApiKey = getString("macromeasures.prod.api_key")
  val mmTestApiKey = getString("macromeasures.test.api_key")

  val mmBatchSize = getInt("macromeasures.api.batch_size")
  val mmRetryLimit = getInt("macromeasures.api.retry_limit")
  val mmRetryDelay = getInt("macromeasures.api.retry_delay")

  // partner xref details
  val partnerXrefFiles = getMap[String]("partner_xref.file_map")
  val partnerIdColumnName = getString("partner_xref.partner_id_col")
  val internalIdColumnName = getString("partner_xref.internal_id_col")
  val partnerXrefColumns = List(partnerIdColumnName, internalIdColumnName)

}
