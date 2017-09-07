package com.spotright.trickshot.lib

import scala.concurrent.ExecutionContextExecutor

import com.datastax.spark.connector.cql.CassandraConnector
import com.spotright.common.concurrent.HighWaterQueue
import com.spotright.xcass._


/**
  * Created by nhalko on 12/21/16.
  */

object CassFIFO {

  final val hiwater = 100
  type HWQ = HighWaterQueue[StmtExec]

  // Use this locally
  def apply(conn: CassClient, hiwater: Int)(implicit ec: ExecutionContextExecutor): HWQ = {
    new HighWaterQueue[StmtExec](
      hiwater = hiwater,
      ops => conn.withSessionDo { implicit session: Session =>
        SessionOps.submit(ops.map(_.executeAsync()))
      }
    )
  }

  def apply(conn: CassClient)(implicit ec: ExecutionContextExecutor): HWQ = apply(conn, hiwater)

  // use this in Spark setting
  def apply(conn: CassandraConnector, hiwater: Int)(implicit ec: ExecutionContextExecutor): HWQ = {
    new HighWaterQueue[StmtExec](
      hiwater = hiwater,
      ops => conn.withSessionDo { implicit session: Session =>
        SessionOps.submit(ops.map(_.executeAsync()))
      }
    )
  }

  def apply(conn: CassandraConnector)(implicit ec: ExecutionContextExecutor): HWQ = apply(conn, hiwater)
}