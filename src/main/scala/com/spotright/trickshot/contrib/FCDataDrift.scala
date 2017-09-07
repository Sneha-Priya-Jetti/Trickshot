package com.spotright.trickshot.contrib

import com.spotright.common.util.LogHelper
import com.spotright.trickshot.spark.CanSparkContext
import com.spotright.trickshot.util.HelperMethods
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by sjetti on 1/20/17.
  */
object FCDataDrift extends CanSparkContext with LogHelper {

  def main(args: Array[String]) {

    withSparkSession("FullContacts Data Drift") {
      ss =>
        worker(ss)
    }
  }

  def worker(ss: SparkSession): Unit = {
    implicit val iss = ss

    val sc = ss.sparkContext

    val ADDED = sc.longAccumulator("Added Emd5's")
    val DELETED = sc.longAccumulator("Deleted Emd5's")
    val CONSTANT = sc.longAccumulator("Emd5's with no change")
    val TWID_CHANGE = sc.longAccumulator("Different twid churn")

    val Data1 = linkedFullContacts(s"${HelperMethods.getFCFile(1)}/linked.csv", sc) // old file
    val Data2 = linkedFullContacts(s"${HelperMethods.getLatestFullContactPath}/linked.csv", sc) //latest files

    Data2.cogroup(Data1).foreachPartition {
      iter =>
        iter.foreach {
          case (currentEmd5, (currentTwid, oldTwid)) =>
            if (oldTwid.isEmpty && currentTwid.nonEmpty) ADDED.add(1)
            else if (currentTwid.isEmpty && oldTwid.nonEmpty) DELETED.add(1)
            else if (currentTwid.toSet == oldTwid.toSet) CONSTANT.add(1)
            else TWID_CHANGE.add(1)
        }
    }

    println(s"Total unique FC emd5 --> twid in latest ${Data2.distinct().count()}")
    println(s"EMd5's Added,${ADDED.value}")
    println(s"Emd5's Deleted,${DELETED.value}")
    println(s"Emd5's No Change,${CONSTANT.value}")
    println(s"Emd5's with Twid churn,${TWID_CHANGE.value}")
  }

  def linkedFullContacts(fileName: String, sc: SparkContext): RDD[(String, String)] = {

    sc.textFile(s"$fileName", 3072)
      .mapPartitions {
        iter =>
          iter.map {
            line =>
              val row = line.split(",")
              row(0) -> row(1)
          }
      }
  }
}
