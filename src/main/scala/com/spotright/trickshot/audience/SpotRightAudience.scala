package com.spotright.trickshot.audience

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import java.nio.charset.StandardCharsets

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.spotright.common.json.SprayJsonXPath._
import com.spotright.trickshot.spark.CanSparkContext
import com.spotright.trickshot.util.Config
import org.apache.spark.SparkContext
import scopt.OptionParser
import spray.json.DefaultJsonProtocol._
import spray.json._

/**
  * Created by sjetti on 12/8/16.
  */
object SpotRightAudience extends CanSparkContext {

  case class Opts(inputFile: String = ""
                  , historyFile: String = ""
                  , outputPath: String = ""
                  , appUri: String = "https://app.spotright.com")

  val optParser = new OptionParser[Opts]("Indexing Audience to Excel") {

    opt[String]("inputFile") text "HDFS URI to indv_data JSON file" required() action { (p, c) => c.copy(inputFile = p) }
    opt[String]("historyFile") text "HDFS URI to indv_data JSON file" required() action { (p, c) => c.copy(historyFile = p) }
    opt[String]("output") text "HDFS URI to output excel file" required() action { (p, c) => c.copy(outputPath = p) }
    opt[String]("appUri") text "URL of front-end webapp" action { (p, c) => c.copy(appUri = p) }
  }

  val fsPrefix = "s3n://"
  val graphmassicebucket = "graphmassive"

  def main(args: Array[String]) {
    val opts = optParser.parse(args, Opts()).get
    withSparkContext("SpotRight Audience") {
      sc =>
        worker(opts, sc)
    }
  }


  def worker(opts: Opts, sc: SparkContext) = {
    implicit val isc = sc

    val history = getJobHistory(opts.inputFile, opts.historyFile, opts.appUri)

    setAWSKeys(sc, Config.awsAccess, Config.awsSecret)
    val s3Client = new AmazonS3Client(new ProfileCredentialsProvider())

    history.foreach {
      case (name, baseuri) =>
        val (bucket, path) = getBucketAndPath(s"$baseuri/emailmd5s.csv")

        val s3data =
          try s3Client.getObject(bucket, path).getObjectContent
          catch {
            case e: Exception =>
              sys.error(s"key=$path aws_message=${e.getMessage} $name")
          }

        val fh = new BufferedReader(new InputStreamReader(s3data, StandardCharsets.UTF_8))

        val emd5s = Iterator.continually(fh.readLine())
          .takeWhile(_ != null)

        sc.parallelize(emd5s.toSeq, 2048).saveAsTextFile(s"${opts.outputPath}/$name")

        fh.close()
        s3data.close()
    }
  }

  def getJobHistory(inputFile: String, historyFile: String, appUri: String): Map[String, String] = {

    val jobs = scala.io.Source.fromFile(inputFile).getLines().toArray

    scala.io.Source.fromFile(historyFile).getLines().mkString("\n").parseJson
      .convertTo[List[JsObject]]
      .flatMap {
        obj =>
          val jobId = (obj \ "id").toString().replace("\"", "")
          if (jobs.contains(jobId)) {
            val submissionDetails = (obj \ "spark_job_server_submission" \ "sparkianda").convertTo[Map[String, JsValue]]
            val data = (obj \ "ianda_submission").convertTo[Map[String, JsValue]]
            Some(s"$jobId-${submissionDetails("segment").toString().replace("\"", "")}" -> data("base_uri").toString().replace("\"", ""))
          }
          else None
      }
      .toMap

    /*(scala.io.Source.fromURL(s"$appUri/api/v3/history").getLines().mkString("\n").parseJson
      \ "data")
      .convertTo[List[JsObject]]
      .flatMap {
      obj =>
        val jobId = (obj \ "jobid").toString().replace("\"", "")
        if (jobs.contains(jobId)){
          val submissionDetails = (obj \ "sparkJobserverSubmission" \ "sparkianda").convertTo[Map[String, JsValue]]
          val data = (obj \ "iandaSubmission").convertTo[Map[String, JsValue]]
          Some(s"$jobId / ${submissionDetails("segment").toString().replace("\"", "")}" -> data("base_uri").toString().replace("\"", ""))
        }
        else None
    }
    .toMap*/
  }

  def getBucketAndPath(uri: URI): Array[String] = {
    if (uri.getHost != null && uri.getHost == "s3.amazonaws.com")
      uri.getPath.stripPrefix("/").split("\\/", 2)
    else {
      val b = uri.getHost.stripSuffix(".s3.amazonaws.com")
      val p = uri.getPath.stripPrefix("/")

      assert(!b.isEmpty, "empty bucket")

      Array(b, p)
    }
  }

  def getBucketAndPath(uri: String): (String, String) = {
    val Array(bucket, path) = getBucketAndPath(new URI(uri))
    bucket -> path
  }

}
