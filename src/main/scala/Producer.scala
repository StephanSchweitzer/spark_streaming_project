import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Producer
{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("YourApp")
      .master("local[*]") // Use local mode for running Spark locally
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits

    println("Hello world ")

  }

}
