import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SparkApp extends App{

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("test-app")
    .master("local")
    .getOrCreate()

  import sparkSession.implicits._

  val hotels = sparkSession.read.format("csv")
    .option("header" : True)
    .load("src/main/resources/m06sparkbasics/hotels/")

  hotels.show()

  val weather = sparkSession.read.load("src/main/resources/m06sparkbasics/weather/")
  weather.show()
  weather.printSchema()

  private val APIKey = ""

  val hotelClearDF = hotels.





}
