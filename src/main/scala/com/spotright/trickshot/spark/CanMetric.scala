package com.spotright.trickshot.spark


import scala.collection.mutable.{Set => MSet, HashSet => MHashSet, Queue => MQueue}

import java.util.concurrent.TimeUnit
import java.util.{SortedMap => JSMap}

import spray.json._
import spray.json.DefaultJsonProtocol._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext
import com.codahale.metrics._
import com.spotright.common.concurrent.HighWaterQueue
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

/**
  * Created by nhalko on 6/25/14.
  *
  * Sets up metrics of the form:
  *
  * ${appName}.SpotRightMetrics.${type}.${metricName}
  */

trait CanMetric {

  val srMetrics = "SpotRightMetrics"
  type HWQI = HighWaterQueue[Int]
  type AccumulableSet = AccumulableV2Set[String]
  type AccumulatorV2Buckets = List[(Int, LongAccumulator)]

  def counter(name: String, init: Long = 0L)(implicit sc: SparkContext): LongAccumulator = {
    //    val metricRegistry = SparkEnv.get.metricsSystem.registry
    //    val metricPfx = s"${sc.appName}.${srMetrics}.counter"
    val metric = {
      val m = sc.longAccumulator(name)
      if (init != 0L) m.add(init)
      m
    }
    //
    //    metricRegistry.register(MetricRegistry.name(metricPfx, name), new Gauge[Long] {
    //      override def getValue: Long = metric.value
    //    })

    metric
  }

  val accumulatorRegistry = MSet.empty[LongAccumulator]

  def accumulableSet(implicit sc: SparkContext): AccumulableV2Set[String] = {
    val acc = new AccumulableV2Set[String]
    sc.register(acc)
    acc
  }

  /**
    * Use this accumulator if you want to use the methods below.  We keep our own
    * accumulator registry to easily fish out our accumulators for printing or monitoring.
    */
  def longAccumulator(name: String)(implicit sc: SparkContext): LongAccumulator = {
    val acc = sc.longAccumulator(name)
    accumulatorRegistry += acc
    acc
  }

  /**
    * logger.info(accumulators2Json().prettyPrint)
    */
  def accumulators2Json(): JsObject = {
    JsObject(
      accumulatorRegistry.map { acc =>
        acc.name.get.replace(" ", "_") -> JsNumber(acc.value)
      }
        .toMap
    )
  }

  /**
    * Periodically print out accumulator values to console
    * for a good feeling of progress.
    */
  def accumulatorsListen(pollSeconds: Int = 600): Unit = {

    def listen(): Unit = {
      while (true) {
        Thread.sleep(pollSeconds * 1000)
        println(accumulators2Json().prettyPrint)
      }
    }

    val alThread = new Thread("accumulators-listen") {
      override def run() { listen() }
    }
    alThread.setDaemon(true)
    alThread.start()
  }

  /**
    * ToDo: coda hale metrics cannot be exposed to spark jobs because of serialization errors
    * so this is broken
    */
  //  def histogram(name: String)(implicit ssc: StreamingContext) = {
  //    val metricRegistry = SparkEnv.get.metricsSystem.registry
  //    val metricPfx = s"${ssc.sparkContext.appName}.${srMetrics}.histogram"
  //
  //    val hist = metricRegistry.register(MetricRegistry.name(metricPfx, name), new Histogram(new ExponentiallyDecayingReservoir()))
  //    val histQueue = new HighWaterQueue[Int](
  //      hiwater = 128,
  //      counts => counts.foreach(hist.update)
  //    )
  //
  //    implicit object HWQAcc extends AccumulableParam[HWQI, Int] {
  //      def addAccumulator(r: HWQI, t: Int) = {
  //        r += t
  //        r
  //      }
  //
  //      def addInPlace(r1: HWQI, r2: HWQI) = {
  //        r1.drain()
  //        r2.drain()
  //        histQueue
  //      }
  //
  //      def zero(initialValue: HWQI) = histQueue
  //    }
  //
  //    ssc.sparkContext.accumulable[HWQI, Int](histQueue)
  //  }

}


class AccumulableV2Set[K] extends AccumulatorV2[K, MHashSet[K]] {

  private var _value: MHashSet[K] = MHashSet.empty[K]

  def isZero: Boolean = _value.isEmpty

  def add(in: K): Unit = _value += in

  def copy(): AccumulableV2Set[K] = {
    val next = new AccumulableV2Set[K]
    next.merge(this)
    next
  }

  def merge(other: AccumulatorV2[K, MHashSet[K]]): Unit = _value ++= other.value

  def reset(): Unit = _value.clear()

  def value: MHashSet[K] = _value
}

object CanMetric extends CanMetric

/**
  * !!! Both of these implementation are very sensitive to Serializable errors, most likely
  * due to the SparkContext brought in to create the accumulators (which is why they are broken
  * out into companion objects)
  *
  * Given buckets List(5, 10, 50), produce counts
  * [0,5)
  * [5,10)
  * [10,50)
  * >= 50
  */
class HistogramAcc(val histMap: List[(Int, LongAccumulator)]) extends Serializable {

  def +=(i: Int): Unit = {
    if(i == 0) {
      histMap.find {
        i == _._1
      } match {
        case Some(acc) => acc._2.add(1) // we hit a bucket
        case None => histMap.last._2.add(1) // incr the overflow bucket
      }
    }
    else if(i < 0) {
      histMap.find {
        i <= _._1
      } match {
        case Some(acc) => acc._2.add(1) // we hit a bucket
        case None => histMap.last._2.add(1) // incr the overflow bucket
      }
    }
    else {
      histMap.find {
        i < _._1
      } match {
        case Some(acc) => acc._2.add(1) // we hit a bucket
        case None => histMap.last._2.add(1) // incr the overflow bucket
      }
    }

  }

  // must have the same buckets
  def ++(that: HistogramAcc): Unit = {
    val newHistMap = (this.histMap zip that.histMap).map {
      case ((b, acc1), (_, acc2)) =>
        acc1.add(acc2.value)
        b -> acc1
    }.sortBy(_._1)

    new HistogramAcc(newHistMap)
  }

  override def toString: String = {
    histMap.map {
      case (_, acc) =>
        s"  ${acc.name.get}     ${acc.value}"
    }.mkString("\n")
  }
}

object HistogramAcc {
  def createBuckets(buckets: List[Int], name: String = "")(implicit sc: SparkContext): List[(Int, LongAccumulator)] = {
    List(buckets.head -> sc.longAccumulator(s"hist: $name - [-inf, ${buckets.head})")) ++
      buckets.sliding(2).map {
        case List(a, b) =>
          b -> sc.longAccumulator(s"hist: $name - [$a, ${b - 1})")
      }.toList ++ // list keeps it ordered
      List(buckets.last -> sc.longAccumulator(s"hist: $name - [${buckets.last}, inf)"))
  }

  def createNegBuckets(buckets: List[Int], name: String = "")(implicit sc: SparkContext): List[(Int, LongAccumulator)] = {
    List(buckets.head -> sc.longAccumulator(s"hist: $name - [-inf, ${buckets.head})")) ++
      buckets.sliding(2).map {
        case List(a, b) =>
          b -> sc.longAccumulator(s"hist: $name - [${a +1}, $b)")
      }.toList
  }

  def createPosBuckets(buckets: List[Int], name: String = "")(implicit sc: SparkContext): List[(Int, LongAccumulator)] = {
    buckets.sliding(2).map {
      case List(a, b) =>
        b -> sc.longAccumulator(s"hist: $name - [$a, ${b -1})")
    }.toList ++ // list keeps it ordered
      List(buckets.last -> sc.longAccumulator(s"hist: $name - [${buckets.last}, inf)"))
  }

  def createZeroBucket(buckets: List[Int], name: String = "")(implicit sc: SparkContext): List[(Int, LongAccumulator)] = {
    List(buckets.head -> sc.longAccumulator(s"hist: $name - [0, 0)"))
  }

  def combineBuckets(oldBucket: List[(Int, LongAccumulator)], newBucket: List[(Int, LongAccumulator)]): List[(Int, LongAccumulator)] = {
    oldBucket ::: newBucket
  }
}

/**
  * Cumulative distribution (histogram really) measures all values less than or equal to the bucket boundaries.
  * Set complimentary = true to measure all values greater than or equal to the bucket.
  */
class CumulativeDistAcc(histMap: List[(Int, LongAccumulator)], complimentary: Boolean = false) extends Serializable {

  def op(x: Int, y: Int): Boolean = if (complimentary) x > y else x <= y

  def +=(i: Int): Unit = {
    val wanted = histMap.filter(h => op(i, h._1))

    if (wanted.isEmpty)
      histMap.last._2.add(1) // incr overflow bucket
    else
      wanted.foreach(_._2.add(1)) // incr wanted buckets
  }

  override def toString: String = {
    histMap.map {
      case (_, acc) =>
        s"  ${acc.name.get}     ${acc.value}"
    }.mkString("\n")
  }
}

object CumulativeDistAcc {
  def createBuckets(buckets: List[Int], name: String = "", complimentary: Boolean = false)(implicit sc: SparkContext)
  : List[(Int, LongAccumulator)] = {

    (0 :: buckets).sliding(2).map {
      case List(a, b) =>
        (if (complimentary) a else b) -> sc.longAccumulator(s"cumulativeDist: $name - [$a, $b)")
    }.toList ++ // list keeps it ordered
      List(buckets.last -> sc.longAccumulator(s"cumulativeDist: $name - [${buckets.last}, inf)"))
  }
}
