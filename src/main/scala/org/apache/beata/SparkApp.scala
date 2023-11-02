package org.apache.beata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}


object SparkApp extends App{

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("test-app")
    .master("local")
    .getOrCreate()

  sparkSession.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")

  val key = AccessService.getAccess("azkey")

  sparkSession.conf.set("fs.azure.account.key.hotelsweather.blob.core.windows.net", key)

  val hotels = sparkSession.read
    .format("csv")
    .option("header", "True")
//        .load("src/main/resources/m06sparkbasics/hotels/")
    .load("wasbs://hotwea@hotelsweather.blob.core.windows.net/m06sparkbasics/hotels")

    hotels.show()
    hotels.printSchema()

  import org.apache.spark.sql.functions.udf

  val geoLat = udf(GeoService.getGeolocation(_: String, _: String, _: String)._1)
  val geoLng = udf(GeoService.getGeolocation(_: String, _: String, _: String)._2)

  val hotelsCleanDf = hotels
    .withColumn("Latitude",
      when(col("Latitude").isNaN,
        geoLat(col("country"), col("city"), col("address")))
        .otherwise(col("Latitude")))
    .withColumn("Longitude",
      when(col("Longitude").isNaN,
        geoLat(col("country"), col("city"), col("address")))
        .otherwise(col("Longitude")))

  //  hotelsCleanDf.show()

  val weather = sparkSession.read
    .format("parquet")
//        .load("src/main/resources/m06sparkbasics/weather/")
    .load("wasbs://hotwea@hotelsweather.blob.core.windows.net/m06sparkbasics/weather")
    weather.show()
//    weather.printSchema()


  val hashUDF = sparkSession.udf.register("hash", GeoService.getGeohash(_: Double, _: Double): String)
  val weatherWithHash = weather.withColumn("hashed", hashUDF(col("lat"), col("lng")))
  val hotelsWithHash = hotelsCleanDf.withColumn("ha", hashUDF(col("Latitude"), col("Longitude")))

    weatherWithHash.show()
  //  hotelsWithHash.show()


  val joined2 = hotelsWithHash.join(weatherWithHash, hotelsWithHash("ha") === weatherWithHash("hashed"), "left").select("City")

  joined2.show()
  joined2.write.format("parquet").save("wasbs://hotwea@hotelsweather.blob.core.windows.net/m06sparkbasics/results")

  //  joined.printSchema()
}
