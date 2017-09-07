package com.spotright.trickshot.delivery.fc

import spray.json._

object Emd5Handle {

  def main(av: Array[String]): Unit = {
    require(av.length > 0, "usage: Emd5Handle fcfile")

    val lines = scala.io.Source.fromFile(av(0)).getLines()

    lines.foreach { text =>
      text.split("\t", 4) match {
        case Array(_, _, emd5, jstext) =>
          jstext.parseJson match {
            case JsObject(fs) =>
              fs.get("socialProfiles") match {
                case Some(JsArray(xs)) =>
                  val twits =
                    xs.filter {
                      case JsObject(gs) if gs.get("type").contains(JsString("twitter")) => true
                      case x => false
                    }

                  twits.foreach {
                    case JsObject(gs) =>
                      val handle = gs.get("username").fold("") {
                        case JsString(hand) => hand.toLowerCase
                        case _ => ""
                      }

                      val id = gs.get("id").fold("") {
                        case JsString(ident) => ident
                        case _ => ""
                      }

                      println(s"$emd5,$handle,$id")

                    case _ => ()
                  }

                case _ => ()
              }
            case _ => ()
          }
        case _ => ()
      }
    }
  }
}
