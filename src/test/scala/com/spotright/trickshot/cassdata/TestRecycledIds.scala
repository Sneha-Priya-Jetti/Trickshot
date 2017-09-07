package com.spotright.trickshot.cassdata

/**
  * Created by nhalko on 3/28/14.
  */


import com.spotright.common.util.LogHelper
import com.spotright.trickshot.SparkTester


class TestRecycledIds extends SparkTester with LogHelper {

  "RecycledIds" should {
    "run" in {

      testSpark("TestRecycledIds") {
        sc =>
          RecycledIds.worker(sc, RecycledIds.Opts(targetDir(s"RecycledIds-${System.currentTimeMillis()}")))
      }

      ok
    }
  }

}
