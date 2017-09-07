package com.spotright.trickshot.lib

import scala.util.{Failure, Success, Try}

import scalaj.http._
import spray.json._

import com.spotright.common.util.LogHelper
import com.spotright.common.json.SprayJsonXPath._

import com.spotright.models.third_party.macromeasures.{RateLimitResponse => MMRateResponse}
import com.spotright.models.third_party.macromeasures.MacromeasuresAPIJsonProtocol._

import com.spotright.trickshot.util.{Config, Helpers}

object MacromeasuresDAO extends LogHelper {

  type InstagramID = String

  val apiUrl: String = Config.mmApiUri

  def getRateLimit(apiKey: String): Option[MMRateResponse] = {

    val endpoint = Config.mmLimitsEndpoint

    val req = Http(s"$apiUrl/$endpoint")
      .params("key" -> apiKey)

    val mResult = Try { req.asString.throwError.body }.toOption

    mResult.map { v => (v.parseJson \ "instagram").convertTo[MMRateResponse] }
  }

  def getInstagramDataForIds(ids: Seq[InstagramID], apiKey: String, tries: Int = 1): Option[JsValue] = {

    val batchSize= Config.mmBatchSize
    val retryLimit= Config.mmRetryLimit
    val retryDelay= Config.mmRetryDelay
    val endpoint = Config.mmUsersEndpoint

    require(ids.length <= batchSize, s"No more than $batchSize IDs may be sent to the API at once.")

    val req = Http(s"$apiUrl/$endpoint")
      .params(
        "key" -> apiKey,
        "ids" -> ids.mkString(",")
      )

    val logQuery = s"${ids.take(3).mkString(",")},..."

    val jsResponse = Try {
      req.asString.throwError.body
    } match {
      case Success(resp) => resp.parseJson

      case Failure(ex)   =>
        logger.error(s"Error when calling API: ${Helpers.errorMsgAndStackTrace(ex)}")
        logger.error(s"Query: {}, Try: {}", logQuery, tries)
        JsObject.empty
    }

    val completeResponse = Try { (jsResponse \ "complete").convertTo[Boolean] }.getOrElse(false)

    // Get the wait time
    val rateLimit = getRateLimit(apiKey)
    val waitTime = rateLimit.map(_.period_reset).getOrElse(60)
    val remaining = rateLimit.map(_.period_remaining).getOrElse(0)

    // The response from the Macromeasures API may come back as "complete":false indicating that they are
    // refreshing user data for our queries in the background. Retry up to retryLimit times to get complete data.
    if (completeResponse) {
      logger.info(s"API returned complete response. Query: {}, Try: {}", logQuery, tries)

      if (remaining < batchSize) {
        logger.info(s"Waiting $waitTime seconds...")
        Thread.sleep(waitTime * 1001L)
      }

      Some(jsResponse)
    } else if (tries >= retryLimit) {
      logger.warn(s"API returned incomplete response after $retryLimit tries; not retrying. Query: {}, Try: {}", logQuery, tries)
      Option.empty[JsValue]
    } else {
      logger.info(s"API returned incomplete response; retrying in $retryDelay seconds. Query: {}, Try: {}", logQuery, tries)
      Thread.sleep(retryDelay * 1001L)
      getInstagramDataForIds(ids, apiKey, tries + 1)
    }
  }
}