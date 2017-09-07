package com.spotright.trickshot.cassdata

import java.time._

import com.spotright.trickshot.spark.CanSparkContext
import com.spotright.trickshot.util.Config
import com.spotright.xcass.SpotCass
import com.spotright.xcass.spark.CanCass
import org.apache.spark.SparkContext

/**
 * Created by sjetti on 1/17/17.
 */
object TwitterDuplicateId extends CanSparkContext with CanCass {

  def main(args: Array[String]) {
    withSparkContext("TwitterDuplicateId") {
      sc =>
        worker(sc)
    }
  }

  /**
   * Create a report with TwitterID, handle and timestamp of twitter Id
   * to find duplicate twitter Id's and latest among the duplicates
   */
  def worker(sc: SparkContext): Unit = {
    implicit val isc = sc

    createCassRDD(SpotCass.twitter, where = "column1 = 'id'")
      .flatMap {
        case (key, columns) =>

          def getLatest(field: String): Option[(String, Instant)] = {
            columns.filter(_.column1 == field).sortBy(_.timestamp).lastOption.map{ icol =>
              icol.column2 -> icol.tsInstant
            }
          }

          for {
            (id, time) <- getLatest("id")
          } yield {
            id -> (key -> time)
          }
      }
      .groupByKey()
      .sortBy{case (id, _) => id}
      .filter(duplicates => duplicates._2.toMap.keySet.size > 1)
      .flatMap {
        case (id, handles) =>
          handles.map {
            case (handle, time) =>
            s"$id,$handle,$time"
          }
      }
      .coalesce(10)
      .saveAsTextFile(s"${Config.hdfs}/user/nutch/SnehaProj/DuplicatedTwid-${System.currentTimeMillis()}")
  }
}
