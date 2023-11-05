package org.apache.beata

import com.typesafe.scalalogging.Logger

import java.io.{FileInputStream, IOException, InputStream}
import java.util.Properties
import scala.io.Source

object AccessService {

  def getAccess(key_name: String):String = {
    try {
      val input = new FileInputStream("application.properties")
      try {
        val prop = new Properties
        prop.load(input)
       prop.getProperty(key_name)
      } catch {
        case ex: IOException =>
          "File not found"
      } finally if (input != null) input.close()
    }
  }





}
