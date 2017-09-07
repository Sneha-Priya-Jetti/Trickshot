package com.spotright.trickshot.util

import scala.util.Try

import com.spotright.common.json.SprayJsonXPath._
import com.spotright.common.util.LogHelper
import spray.json.DefaultJsonProtocol._

import spray.json._

/**
  * SR-485 create a single excel file for indv_data.json and insights.json
  * Created by sjetti on 3/22/16.
  */
object ExcelHelper extends LogHelper {

  final val MAX_SHEET_NAME_CHARS = 29
  final val MAX_BRANDS = 500
  final val MAX_INTERESTS = 500

  // Field names for state, metro area, and DMA must be hard-coded due to a front-end design decision
  // See comment in indvDict()
  final val DLX_STATE_FIELD = "g_state"
  final val EXP_STATE_FIELD = "state"
  final val DLX_METRO_FIELD = "residence_metro_area"
  final val EXP_METRO_FIELD = "metro_area"
  final val DLX_DMA_FIELD = "g_dma_code"

  def indvDict(appUri: String, demogSource: String): List[(Array[String], List[String])] = {

    (scala.io.Source.fromURL(s"$appUri${Config.indvDictionaryEndpoint}?datasource=$demogSource").getLines.mkString("\n").parseJson
      \ "data" \ "indv_data_dictionary")
      .convertTo[Map[String, Map[String, JsValue]]]
      .flatMap {
        case (fieldName, data) =>
          val title = Try {
            prettyTitleFromSlug(data("name").convertTo[String])
          }
            .toOption.getOrElse(data("title").convertTo[String]).trim
          val displayable = ListIfNotNull(data("displayable")).map(_.trim).contains("excel_xml").toString
          val category = StringIfNotNull(data("category")).trim
          val childrenList = ListIfNotNull(data("children")).map(_.trim)
          val dataSource = data("datasource").convertTo[String]
          val valuesList = data("list").convertTo[List[Map[String, JsValue]]]

          valuesList.map {
            valueData =>
              val valueName = valueData("title").convertTo[String].trim
              val rawValue = valueData("value") match {
                case v: JsNumber => v.convertTo[Int].toString
                case v: JsString => v.convertTo[String].trim
                case _ => sys.error("Unknown value type")
              }

              (Array(title, fieldName, rawValue, valueName, displayable, category, dataSource), childrenList)
          }
      }.toList ++ geoFields(demogSource)
  }

  /**
    * We hard-code state, metro area, and DMA because they have many possible values that do not require translation
    * and are not included in the data dictionary. Values for all demog sources are specified here -- the
    * appropriate values are filtered out later.
    */
  def geoFields(demogSource: String): List[(Array[String], List[String])] = demogSource match {

    case "standard" => List(
      (Array("State", DLX_STATE_FIELD, "state", "state", "true", "Geo", "standard"), List.empty[String]),
      (Array("Metro Area", DLX_METRO_FIELD, "metro_area", "metro_area", "true", "Geo", "standard"), List.empty[String])
    ) ++ dmaCodes
    case "partner1" => List(
      (Array("State", EXP_STATE_FIELD, "state", "state", "true", "Geo", "partner1"), List.empty[String]),
      (Array("Metro Area", EXP_METRO_FIELD, "metro_area", "metro_area", "true", "Geo", "partner1"), List.empty[String])
    )
    case _ => List.empty[(Array[String], List[String])]
  }

  /**
    * A CSV map of Nielson DMA codes and their descriptions is located at src/main/resources/dma_description.csv.
    * We use this to generate the display values for DMA code, which are not included in the data dictionary.
    */
  val dmaCodes: List[(Array[String], List[String])] = {
    val csvURL = getClass.getClassLoader.getResource("dma_description.csv")
    val iter = scala.io.Source.fromURL(csvURL, "UTF-8").getLines()

    // skip the header
    iter.next()

    iter.map {
      line =>
        val Array(code, description) = line.split(",", 2)

        (Array("DMA", DLX_DMA_FIELD, code, description, "true", "Geo", "standard"), List.empty[String])
    }
      .toList
  }

  /**
    * HELPERS
    */

  def ListIfNotNull(l: JsValue): List[String] = if (l != JsNull) l.convertTo[List[String]] else List.empty[String]

  def StringIfNotNull(l: JsValue): String = if (l != JsNull) l.convertTo[String] else "Indiv"

  def prettyTitleFromSlug(sl: String): String = sl.replace("_", " ").capitalize

}
