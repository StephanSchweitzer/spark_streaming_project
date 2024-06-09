import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType




object Consumer {
      def main(args: Array[String]): Unit = {
        val host = "localhost"
        val port = 9999

        //to make the project run on windows you need this folder with winutils.ext and hadoop.dll to be linked
        //System.setProperty("hadoop.home.dir", "resources/hadoop")

        val spark = SparkSession
          .builder
          .appName("Consumer")
          .master("local[*]")
          .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")


        val socketDF = spark.readStream
          .format("socket")
          .option("host", host)
          .option("port", port)
          .load()

        socketDF.isStreaming
        socketDF.printSchema

        val csvDirectory = "produced_data"
        val hateSpeechSchema = new StructType().add("id", "integer").add("text", "string").add("hateful", "integer")


//        val staticDF = spark.read
//          .format("csv")
//          .option("header", "true")
//          .option("recursiveFileLookup", "true")
//          .schema(hateSpeechSchema)
//          .load(csvDirectory)
//
//        staticDF.show()

        val csvDF = spark.readStream
          //.format("csv")
          .option("sep", ",")
          .option("header", "true")
          .schema(hateSpeechSchema)
          .option("recursiveFileLookup", "true")
          //.load(csvDirectory)
          .csv(csvDirectory)

        csvDF.printSchema()

        val query = csvDF.writeStream
          .outputMode("append") // Use "append" for appending new rows as they arrive
          .format("console") // Output to the console
          .start()

        query.awaitTermination()
//
       import spark.implicits._
//

//
//        // Split the lines into words
//        val words = lines.as[String].flatMap(_.split(" "))
//
//        // Generate running word count
//        val wordCounts = words.groupBy("value").count()
//
//        // Start running the query that prints the running counts to the console
//        val query = wordCounts.writeStream
//          .outputMode("complete")
//          .format("console")
//          .start()
//
//        query.awaitTermination()
      }
}
