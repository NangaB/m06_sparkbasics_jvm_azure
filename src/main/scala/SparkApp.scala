import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SparkApp extends App{

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("test-app")
    .master("local")
    .getOrCreate()

  val key = "IGJIhgBmC0nHhKXndiirYQy4bMS2SjN/S+441tMeSZmvqAOXyYLFju6o6HNEpj40csfbGeZlIfEX+AStz+uqXg=="

  sparkSession.conf.set("fs.azure.account.key.hotelsweather.dfs.core.windows.net", key)

//  sparkSession.conf.set(s"fs.azure.account.key.$outputStorageAccount", inputSaSharedKeySecret)

  import sparkSession.implicits._

  val hotels = sparkSession.read
    .format("csv")
    .option("header", "True")
//    .load("src/main/resources/m06sparkbasics/hotels/")
    .load("abfss://hotwea@hotelsweather.dfs.core.windows.net/m06sparkbasics/hotels")

  hotels.show()
  hotels.printSchema()

  import org.apache.spark.sql.functions.udf
  val geoUDF = udf(GeoService.getGeolocation(_:String, _:String, _:String))
  
  val hotelsCleanDf = hotels.withColumn("location", geoUDF(col("country"), col("city"), col("address")))

  val weather = sparkSession.read
    .format("parquet")
//    .load("src/main/resources/m06sparkbasics/weather/")
    .load("abfss://hotwea@hotelsweather.dfs.core.windows.net/m06sparkbasics/weather")
  weather.show()
  weather.printSchema()


  val hashUDF = sparkSession.udf.register("hashWeather", GeoService.getGeohash(_:Double, _:Double) : String)
  val weatherWithHash = weather.withColumn("hash", hashUDF(col("lng"), col("lat")))

//  weatherWithHash.show()
//  weatherWithHash.printSchema()
}
