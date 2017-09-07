package com.spotright.trickshot.util

import java.io.FileWriter

import com.opencsv.CSVWriter
import com.spotright.common.util.LogHelper
import com.spotright.common.util.UrlTidy.urlFilterThenNormalize
import com.spotright.models.base.BaseJsonProtocol.twitterMetaFormat
import com.spotright.models.base.TwitterMeta
import com.spotright.xcass.{R1, Session, SpotCass}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import spray.json._

import scala.util.Try

/**
  * Created by sjetti on 7/6/17.
  */
object HelperMethods extends LogHelper{

  /**
    *
    * @param position position of file in array as array[files] is in descending order with latest first
    * @return file name in given position
    */

  def getFCFile(position: Int): String  = {
    val base_file = s"${Config.hdfs}/user/nutch/fc_files/releases"
    val dir = new Path(base_file)

    dir.getFileSystem(new Configuration).listStatus(dir).sortWith(_.getPath.getName > _.getPath.getName)(position).getPath.toString
  }

  /**
    *
    * @return the latest full contact file path
    */
  def getLatestFullContactPath: String = {
    val base_file = s"${Config.hdfs}/user/nutch/fc_files/releases"
    val fcPath = new Path(base_file)

    fcPath.getFileSystem(new Configuration).listStatus(fcPath)
      .filter { file => file.isDirectory && file.getPath.getName.matches("[0-9]{8}") }
      .map(_.getPath.toString)
      .max
  }

  /**
    * Create a csv file
    * @param header header of the file
    * @param input data to be saved
    * @param outputFile output file location
    */
  def writeCSV(header: Array[String], input: Seq[String], outputFile: String): Unit = {

    val writer = new CSVWriter(new FileWriter(outputFile))
    writer.writeNext(header)
    input
      .foreach {
        line =>
          val eachLine = line.split(",")
          writer.writeNext(eachLine)
      }
    writer.close()
  }

  /**
    * CASSANDRA inqueries
    */

  // WARNING: This method is NOT safe, it will throw errors
  def getTwitterMeta(handle: String)(implicit session: Session): TwitterMeta = {
    SpotCass.outlets.selectCol(R1(urlFilterThenNormalize(Helpers.handleToUrl(handle)).get, "meta:twitter"))
      .getOne().get.stringVal.parseJson.convertTo[TwitterMeta](twitterMetaFormat)
  }

  // Always return a TM object, even if its empty
  def getTwitterMetaSafely(handle: String)(implicit session: Session): TwitterMeta = {
    Try {
      getTwitterMeta(handle)
    }.getOrElse(TwitterMeta())
  }
}