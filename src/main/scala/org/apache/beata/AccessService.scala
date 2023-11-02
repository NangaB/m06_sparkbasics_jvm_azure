package org.apache.beata

import com.typesafe.scalalogging.Logger
import org.apache.beata.SparkApp.getClass

import java.util.Properties
import scala.io.Source

object AccessService {

  def getAccess(key_name: String): String ={
    val url = getClass.getResource("config.properties")
    val properties: Properties = new Properties()
    if(url != null){
      val source = Source.fromURL(url)
      properties.load(source.bufferedReader())
    }else{
      val log = Logger("Logger")
      log.error("properties file cannot be find")
    }
    properties.getProperty(key_name)
  }

}
