package com.spotright.trickshot

import com.spotright.common.util.LogHelper
import com.spotright.trickshot.lib.CassFIFO
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import com.spotright.trickshot.spark.CanSparkContext
import com.spotright.xcass.{CassClient, SpotCass}
import com.spotright.models.asty.CassOperation
import com.spotright.xcass.test.SpotCassTestHelper
import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragments


trait BaseTesterMethods extends LogHelper {

  /**
    * Delete a whole solr collection in a test case.
    * NOTE: Solr servers are hard-coded to avoid ever deleting from production
    */
  def wipeSolr(collection: String) {
    //    logger.info(s"Deleting all solr docs in $collection....")
    //    import scala.sys.process._
    //    s"curl -s http://192.168.5.70:8983/solr/$collection/update?stream.body=%3Cdelete%3E%3Cquery%3E*:*%3C/query%3E%3C/delete%3E".run()
    //    s"curl -s http://192.168.5.70:8983/solr/$collection/update?stream.body=%3Ccommit/%3E".run()
    //    sleeping(3)
  }

  def sleeping(seconds: Int, msg: String = "sleeping") {
    System.out.print(s"$msg...")
    (1 to seconds).foreach { _ =>
      Thread.sleep(1000)
      System.out.print(".")
    }
    System.out.println(": done")
  }

  def pauseTest(msg: String = "") = com.spotright.Breakpoint.forCass(msg)

  /**
    * Get fully qualified file name in test cases
    *
    * @param name File name
    * @return fully qualified name
    */
  def resourceFile(name: String) = {

    getClass.getResource(s"/$name").toString
    //s"$fsPfx/${System.getenv("PWD")}/src/test/resources/$name"
  }

  /**
    * Get fully qualified file name in test cases with or without the resource indicator "file:/"
    *
    * @param name File name
    * @return fully qualified name
    */
  def resourceFile(name: String, localFile: Boolean = false) = {

    val resFile = getClass.getResource(s"/$name").toString
    //s"$fsPfx/${System.getenv("PWD")}/src/test/resources/$name"

    if (localFile)
      resFile.stripPrefix("file:")
    else
      resFile
  }

  // get lines of a file in src/test/resources
  def getLines(name: String) = {
    scala.io.Source.fromURL(resourceFile(name)).getLines().toList
  }

  /**
    * Get fully qualified file name in target directory
    *
    * @param fname
    * @return
    */
  def targetDir(fname: String) = s"${System.getenv("PWD")}/target/$fname"

  def ts() = System.currentTimeMillis()

  def writeFile(lines: List[String], fileName: String): Unit = {
    val fid = new java.io.FileWriter(fileName)
    lines.foreach {
      line =>
        fid.write(s"$line\n")
    }
    fid.close()
  }
}

trait BaseTester extends Specification with BaseTesterMethods {

  // collectors of startup/shutdown functions
  var startups = Set.empty[() => Unit]
  var shutdowns = Set.empty[() => Unit]

  override
  def map(fs: => Fragments): Fragments = {
    step {
      startups.foreach(_ ())
    } ^ fs ^
      step {
        shutdowns.foreach(_ ())
      }
  }
}

trait CassTester extends BaseTester {

  startups += { () => {
    // create all tables
    val loadData = CassClient.withSessionDo { implicit session =>
      SpotCassTestHelper.specTestInit()
      SpotCass.outlets.rawCQL("select * from $table").getMany().isEmpty
    }

    // populate test data if not already loaded
    if (loadData) {
      logger.info("Loading cass.test.data")

      val cfifo = CassFIFO(CassClient)

      scala.io.Source.fromFile("src/test/resources/cass.test.data").getLines.foreach {
        line =>
          cfifo += CassOperation.fromString(line).toStmt
      }
      cfifo.drain()
      logger.info("Done loading cass.test.data")
    }
    else {
      logger.info("Skipping cass.test.data load.")
    }
  }
  }

  def testPort(): Int = 9142

  def breakpoint() = {
    com.spotright.Breakpoint.forCass(testPort())
  }
}

trait SparkTester extends CassTester with CanSparkContext {

  /**
    * Wrap a Spark Context for cleanup.
    *
    * @param name Spark context name.
    * @param body Code to run.  You must start() the ssc provided.
    */
  def testSpark[A](name: String)(body: SparkContext => A): A = {
    val rv = withSparkContext(name)(body)

    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")

    rv
  }

  def testSparkSession[A](name: String)(body: SparkSession => A): A = {
    val rv = withSparkSession(name)(body)

    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")

    rv
  }

  /**
    * Make a Spark Stream act as if it's a blocking call.
    *
    * @param name Spark context name.
    * @param ttl  Time to live in millis.
    * @param body Code to run.  You must start() the ssc provided.
    */
  def testSparkStream[A](name: String, ttl: Long)(body: StreamingContext => A): A = {
    var ssc = streamingSparkContext(name)

    try {
      val rv = body(ssc)
      ssc.awaitTerminationOrTimeout(ttl)
      rv
    }
    finally {
      ssc.stop(stopSparkContext = true, stopGracefully = true)
      ssc = null

      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.master.port")
    }
  }
}

