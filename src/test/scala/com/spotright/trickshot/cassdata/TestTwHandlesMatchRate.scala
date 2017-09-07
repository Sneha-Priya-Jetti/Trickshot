package com.spotright.trickshot.cassdata

/**
 * Created by sjetti on 1/13/17.
 */

import com.spotright.trickshot.SparkTester
import org.specs2.mutable.Specification

class TestTwHandlesMatchRate extends Specification with SparkTester {

  sequential

  "GraphDrift" should {

    "run" in {

      testSpark("TestTwHandlesMatchRate") {
        sc =>
          val opts = TwHandlesMatchRate.Opts(handlesList = resourceFile("maxHandles", localFile = true),
            outputFile = s"${System.getenv("PWD")}/target")
          TwHandlesMatchRate.worker(sc, opts)
          TwitterDuplicateId.worker(sc)
      }
      ok
    }
  }
}