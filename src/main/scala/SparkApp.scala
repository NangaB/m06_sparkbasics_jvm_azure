import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SparkApp extends App{

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("test-app")
    .master("local")
    .getOrCreate()

  import sparkSession.implicits._

  val hotels = sparkSession.read.format("csv")
    .load("src/main/resources/m06sparkbasics/hotels/")

  hotels.show()
  hotels.printSchema()

  import org.apache.spark.sql.functions.udf
  val geoUDF = udf(GeoService.getGeolocation(_:String, _:String, _:String))
  
  val hotelsCleanDf = hotels.withColumn("location", geoUDF(col("_c2"), col("_c3"), col("_c4")))
  hotelsCleanDf.show()

  val weather = sparkSession.read.load("src/main/resources/m06sparkbasics/weather/")
//  weather.show()
//  weather.printSchema()

}
