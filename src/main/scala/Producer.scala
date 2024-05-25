import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Producer
{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
    .builder
    .appName("hate_speech_text_producer")
    .config("spark.master", "local")
    .getOrCreate()

    import spark.implicits

    println("Hello world ")

  }

}
