package com.spotright.trickshot.dlx

import com.spotright.common.util.LogHelper
import com.spotright.trickshot.spark.CanSparkContext
import com.spotright.trickshot.util.ExcelHelper._
import com.spotright.models.solr.DemogDLXtoSolrDoc
import com.spotright.models.third_party.demog
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

/**
 * Created by sjetti on 10/13/16.
 */
object FindDLXDrift extends CanSparkContext with LogHelper {
  case class Opts(oldFile: String = "", newFile: String = "", header: String = "", outputFile: String = "")

  val parser = new scopt.OptionParser[Opts]("DLX Drift Finder") {

    arg[String]("<oldfile>") action { (x, c) =>
      c.copy(oldFile = x)
    } text "Path to old distribution File"
    arg[String]("<newfile>") action { (x, c) =>
      c.copy(newFile = x)
    } text "Path to new distribution File"

    arg[String]("<header>") action { (x, c) =>
      c.copy(header = x)
    } text "Path to CSV header"

    arg[String]("<outputfile>") action { (x, c) =>
      c.copy(outputFile = x)
    } text "Path to Output File"
  }

  def main(args: Array[String]): Unit = {
    val opts = parser.parse(args, Opts()).get
    withSparkContext("DLX Data Drift") {
      sc =>
        dirDrift(opts, sc)
    }
  }

  def dirDrift(opts: Opts, sc: SparkContext): Unit = {
    implicit val isc = sc

    val header = scala.io.Source.fromFile(opts.header).getLines().next().split(",")

    val oldData = sc.textFile(opts.oldFile, 3072).map {
      line =>
        val row = line.split(",")
        row(0) -> row.mkString(",")
    }

    val newData = sc.textFile(opts.newFile, 3072).map {
      line =>
        val row = line.split(",")
        row(0) -> row.mkString(",")
    }

    coGroupValues(oldData, newData, header, opts.outputFile, sc)

  }

  def coGroupValues(oldData: RDD[(String, String)], newData: RDD[(String, String)], header: Array[String], outputFile: String, sc: SparkContext): Unit = {

    implicit val isc = sc

    val ADDED = sc.longAccumulator("Added Emd5's")
    val DELETED = sc.longAccumulator("Deleted Emd5's")
    val EMD5COUNT = sc.longAccumulator("Emd5's present in both files")
    val CONSTANT = sc.longAccumulator("Constant Values for emd5's")
    val FIELD_CHANGE = sc.longAccumulator("Different field churn")

    val dictionary = indvDict("https://stage-app.spotright.com", "standard")
    val display = dictionary.flatMap { case(data, children) => if(data(5) == "Demog") Some(data(1)) else None }.toSet
    val fieldLabels = dictionary.map { case (data, children) => data(1) -> data(0) }.toMap
    val fieldCats = dictionary.flatMap { case(data, children) => if(data(5) == "Demog") Some(data(1) -> data(2) -> data(3)) else None}.toMap

    val output = oldData.cogroup(newData)
      .flatMap {
      case (emd5, (oldDLX, newDLX)) =>

        if (oldDLX.nonEmpty && newDLX.nonEmpty) EMD5COUNT.add(1)
        val count = if (oldDLX.isEmpty) {
          ADDED.add(1)
          None
        }
        else if (newDLX.isEmpty) {
          DELETED.add(1)
          None
        }
        else if (newDLX == oldDLX) {
          CONSTANT.add(1)
          None
        }
        else {
          var counters = Seq[String]()
          FIELD_CHANGE.add(1)

          val newValues = getSolrDoc(header, newDLX.head)
          val oldValues = getSolrDoc(header, oldDLX.head)

          newValues.map{
            case(key, value) =>
              if(display.contains(key))
              {
                val old = oldValues.getOrElse(key, "Unk")
                val newValue = if(value == "") "Unk" else value
                if(newValue != old) {
                  if(key == "children_age") {
                    val newFieldValues = header.zip(newDLX.head.split(",")).toMap
                    val oldFieldValues = header.zip(oldDLX.head.split(",")).toMap

                    if(newFieldValues("k_children_0_2") != oldFieldValues("k_children_0_2"))
                      counters :+= s"k_children_0_2,${oldFieldValues("k_children_0_2")},${newFieldValues("k_children_0_2")}"
                    else None
                    if(newFieldValues("k_children_3_5") != oldFieldValues("k_children_3_5"))
                      counters :+= s"k_children_3_5,${oldFieldValues("k_children_3_5")},${newFieldValues("k_children_3_5")}"
                    else None
                    if(newFieldValues("k_children_6_10") != oldFieldValues("k_children_6_10"))
                      counters :+= s"k_children_6_10,${oldFieldValues("k_children_6_10")},${newFieldValues("k_children_6_10")}"
                    else None
                    if(newFieldValues("k_children_11_15") != oldFieldValues("k_children_11_15"))
                      counters :+= s"k_children_11_15,${oldFieldValues("k_children_11_15")},${newFieldValues("k_children_11_15")}"
                    else None
                    if(newFieldValues("k_children_16_18") != oldFieldValues("k_children_16_18"))
                      counters :+= s"k_children_16_18,${oldFieldValues("k_children_16_18")},${newFieldValues("k_children_16_18")}"
                    else None
                  }
                  else counters :+= s"${fieldLabels(key)},${fieldCats.getOrElse((key, old), "Unk")},${fieldCats.getOrElse((key, value),"Unk")}"
                }
                else None
              }
              else None
          }
          Some(counters)
        }
        count
    }.flatMap(identity)

    output
      .map { word => (word, 1) }
      .reduceByKey(_ + _)
      .sortBy(_._1)
      .map(x => x._1 + "," + x._2)
      .coalesce(1)
      .saveAsTextFile(s"$outputFile/FieldsDrift.csv")

    println(s"Added EMD5's ,${ADDED.value}")
    println(s"Deleted EMD5's ,${DELETED.value}")
    println(s"CONSTANT EMD5's ,${EMD5COUNT.value}")
    println(s"No. of EMD5's with same field values ,${CONSTANT.value}")
    println(s"EMD5's with Change ,${FIELD_CHANGE.value}")
  }

  def getSolrDoc(header: Array[String], newDLX: String): Map[String, String] = {

    DemogDLXtoSolrDoc
      .toSolrDoc(demog.DLX(header, newDLX.split(",")))
      .iterator()
      .asScala
      .map(sid => sid.getName -> sid.getValue.toString)
      .toMap
  }
}
