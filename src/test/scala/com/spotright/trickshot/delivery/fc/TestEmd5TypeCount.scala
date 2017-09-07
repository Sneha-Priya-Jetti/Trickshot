package com.spotright.trickshot.delivery.fc

/**
  * Created by sjetti on 9/13/16.
  */

import com.spotright.trickshot.SparkTester
import org.specs2.mutable.Specification

/**
  * Created by sjetti on 9/12/16.
  */
class TestEmd5TypeCount extends Specification with SparkTester {

  sequential

  "Full contact type count" should {

    "run" in {

      val opts = Emd5TypeCount.Opts(resourceFile("fc_datafile.txt"), s"${System.getenv("PWD")}/target")

      testSpark("Test Emd5 Type count") {
        sc =>
          Emd5TypeCount.spark(opts, sc)
      }
      ok
    }
  }
}
