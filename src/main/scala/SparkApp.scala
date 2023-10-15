import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SparkApp extends App{

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("test-app")
    .master("local")
    .getOrCreate()

  import sparkSession.implicits._

  val people: Seq[(String, Int)] = Seq(("marek", 15), ("kasia", 12))
  val peopleDF: Dataset[Row] = people.toDF("name", "age")

  peopleDF.show()

}
