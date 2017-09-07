package com.spotright.trickshot.spark

import com.spotright.trickshot.util.Config
import com.spotright.xcass.spark._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object CanSparkContext extends CanSparkContext

trait CanSparkContext extends CanMetric {

  /**
    *
    * @param appName Name that shows up in UI
    * @param props   Set overrides to any params here
    * @return SparkConf object to feed into the Context creator methods
    */
  def sparkConf(appName: String = s"defaultApp-${System.currentTimeMillis()}",
                props: Map[String, String] = Map()): SparkConf = {

    new SparkConf(loadDefaults = true)
      .setAppName(appName)
      .set("spark.app.id", appName)
      // master has to be set for testing, in prod it can be overridden by --master arg
      // SEE SparkContext.368
      .setIfMissing("spark.master", Config.sparkMaster)
      .set("spark.ui.port", s"${scala.util.Random.nextInt(1000) + 14000}") // set port between 14k-15k
      .set("spark.logConf", "true")
      .setCassConnection()
      .setAll(props)
  }

  /**
    * Provide a SparkContext which gets properly stopped in the event of exception or signal.
    */
  def withSparkContext[A](appName: String)(body: SparkContext => A): A =
    withSparkContext(sparkConf(appName = appName))(body)

  /**
    * Provide a SparkContext which gets properly stopped in the event of exception or signal.
    */
  def withSparkContext[A](sconf: SparkConf = sparkConf())(body: SparkContext => A): A = {
    val sc = new SparkContext(sconf)
    setAWSKeys(sc)

    sys.ShutdownHookThread {
      sc.stop()
    }

    try body(sc)
    finally sc.stop()
  }

  def setAWSKeys(sc: SparkContext, awsAccess: String = Config.awsAccess, awsSecret: String = Config.awsSecret) {
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", awsAccess)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", awsSecret)
  }

  def withSparkSession[A](appName: String)(body: SparkSession => A): A =
    withSparkSession(sparkConf(appName = appName))(body)

  /**
    * Provide a SparkContext which gets properly stopped in the event of exception or signal.
    */

  def withSparkSession[A](sconf: SparkConf = sparkConf())(body: SparkSession => A): A = {

    val ss = SparkSession.builder().config(sconf).getOrCreate()
    val sc = ss.sparkContext

    setAWSKeys(sc)

    sys.ShutdownHookThread {
      ss.stop()
    }

    try body(ss)
    finally ss.stop()
  }

  def streamingSparkContext(appName: String,
                            numReceivers: Int = 1,
                            batchSeconds: Int = Config.sparkDuration,
                            // total rate limit across all receivers
                            rateLimit: Int = Config.sparkStreamingRateLimit,
                            blockIntervalMs: Int = 1000,
                            stopGracefully: Boolean = true,
                            props: Map[String, String] = Map()): StreamingContext = {

    val maxRate = rateLimit / numReceivers

    val sconf = sparkConf(
      appName = appName,
      props = Map(
        // http://spark.apache.org/docs/1.6.1/configuration.html#spark-streaming
        "spark.streaming.receiver.maxRate" -> maxRate.toString,
        "spark.streaming.blockInterval" -> (blockIntervalMs * numReceivers).toString,
        "spark.streaming.backpressure.enabled" -> "true",
        // https://github.com/apache/spark/blob/master/streaming/src/main/scala/org/apache/spark/streaming/scheduler/rate/RateEstimator.scala
        // default is 100 but we want to lower it to 25% of max rate if thats small enough
        "spark.streaming.backpressure.pid.minRate" -> math.min(100, math.max(1, 0.25 * maxRate)).toInt.toString
      ) ++ props
    )

    val ssc = new StreamingContext(new SparkContext(sconf), Seconds(batchSeconds))

    sys.ShutdownHookThread {
      ssc.stop(stopSparkContext = true, stopGracefully = stopGracefully)
    }

    ssc
  }

  /**
    * Helper methods to access conf values
    */

  def cores(implicit sc: SparkContext) = sc.getConf.getInt("spark.cores.max", 1)

  def getFileSystem: FileSystem = {
    val conf = new Configuration()
    conf.set("fs.default.name", Config.hdfs)
    FileSystem.get(conf)
  }

  // stop test jobs and leave prod jobs running
  def awaitTerminate(ssc: StreamingContext, ts: Int = Config.sparkTerminateSecs) {
    if (Config.runMode == "development") ssc.awaitTerminationOrTimeout(ts * 1000) else ssc.awaitTermination()
  }

  // This doesn't really belong in CanSparkContext but an easy way to provide for filesystem or resource (test) data
  /**
    * Given a file URI string or "resource:$path" return a local Filesystem path.
    */
  def uriToPath(uri: String): String = {
    if (uri.startsWith("file:"))
      new java.net.URI(uri).getPath
    else if (uri.startsWith("resource:"))
      this.getClass.getClassLoader.getResource(uri.stripPrefix("resource:")).toURI.getPath
    else sys.error("usage: WordCount resourceOrFileUri [sparkMaster]")
  }
}
