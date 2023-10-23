import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SparkApp extends App{

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("test-app")
    .master("local")
    .getOrCreate()

  sparkSession.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")

  val key = "5VJPsDnb8PuKw463UYcN0q3x4y4XhXOaYTGO/QeYXwq5mGViM4r5jr6Rh4emUUo/uR8jKadot+Cp+AStOe2VJg=="

  sparkSession.conf.set("fs.azure.account.key.hotelsweather.blob.core.windows.net", key)

//  sparkSession.conf.set(s"fs.azure.account.key.$outputStorageAccount", inputSaSharedKeySecret)

  import sparkSession.implicits._

  val hotels = sparkSession.read
    .format("csv")
    .option("header", "True")
//    .load("src/main/resources/m06sparkbasics/hotels/")
    .load("wasbs://hotwea@hotelsweather.blob.core.windows.net/m06sparkbasics/hotels")

//  hotels.show()
//  hotels.printSchema()

  import org.apache.spark.sql.functions.udf

//  val geoUDF = udf(GeoService.getGeolocation(_:String, _:String, _:String))
  val geoLat = udf(GeoService.getGeolocation(_:String, _:String, _:String)._1)
  val geoLng = udf(GeoService.getGeolocation(_:String, _:String, _:String)._2)

//  hotels.map(col("lat").isNaN -> geoUDF(col("country"), col("city"), col("address")))
  hotels.show()
  val hotelsCleanDf = hotels
    .withColumn("Latitude",
    when(col("Latitude").isNaN,
    geoLat(col("country"), col("city"), col("address")))
      .otherwise(col("Latitude")))
    .withColumn("Longitude",
      when(col("Longitude").isNaN,
        geoLat(col("country"), col("city"), col("address")))
        .otherwise(col("Longitude")))

  hotelsCleanDf.show()

  val weather = sparkSession.read
    .format("parquet")
//    .load("src/main/resources/m06sparkbasics/weather/")
    .load("wasbs://hotwea@hotelsweather.blob.core.windows.net/m06sparkbasics/weather")
//  weather.show()
//  weather.printSchema()


  val hashUDF = sparkSession.udf.register("hashWeather", GeoService.getGeohash(_:Double, _:Double) : String)
  val weatherWithHash = weather.withColumn("hash", hashUDF(col("lat"), col("lng")))

//  weatherWithHash.show()

  val joined = hotelsCleanDf.join(weatherWithHash, col("hash"), "left")
//  joined.show()
//  joined.printSchema()
}
