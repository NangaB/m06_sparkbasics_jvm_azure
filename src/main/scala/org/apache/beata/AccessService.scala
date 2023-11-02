package org.apache.beata

import com.typesafe.scalalogging.Logger

import java.util.Properties
import scala.io.Source

object AccessService {

  def getAccess(key_name: String) = {
    val properties = new Properties()
    val file = Source.fromFile("application.properties")
    if (file.isEmpty) {
      val log = Logger("Logger")
      log.error("properties file cannot be find")
    }
      properties.getProperty(key_name)
  }
}
