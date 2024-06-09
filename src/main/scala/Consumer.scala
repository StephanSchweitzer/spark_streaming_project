import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, BooleanType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake
import java.net.URI
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
//implicit val formats: Formats = DefaultFormats


object Consumer {
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def main(args: Array[String]): Unit = {
        val host = "localhost"
        val port = 9999

        //to make the project run on windows you need this folder with winutils.ext and hadoop.dll to be linked
        //System.setProperty("hadoop.home.dir", "resources/hadoop")
        // WebSocket client to send data to Node.js server

        val wsClient = new WebSocketClient(new URI("ws://localhost:3000")) {
          override def onOpen(handshakedata: ServerHandshake): Unit = {
            println("WebSocket connection opened")
          }

          override def onMessage(message: String): Unit = {
            // No need to handle messages from the server
          }

          override def onClose(code: Int, reason: String, remote: Boolean): Unit = {
            println(s"WebSocket connection closed: $reason")
          }

          override def onError(ex: Exception): Unit = {
            ex.printStackTrace()
          }
        }
        wsClient.connect()

          val spark = SparkSession
          .builder
          .appName("Consumer")
          .master("local[*]")
          .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        spark.streams.addListener(new StreamingQueryListener {
          override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
            println(s"Query started: ${event.id}")
          }

          override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
            println(s"Query made progress: ${event.progress}")
            // Send progress data to Node.js server
            wsClient.send(event.progress.json)
          }

          override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
            println(s"Query terminated: ${event.id}")
          }
        })

//        val socketDF = spark.readStream
//          .format("socket")
//          .option("host", host)
//          .option("port", port)
//          .load()
//
//        socketDF.isStreaming
//        socketDF.printSchema

        val csvDirectory = "produced_data"
        val hateSpeechSchema = new StructType().add("id", "integer").add("text", "string").add("hateful", "integer")


        val csvDF = spark.readStream
          .option("sep", ",")
          .option("header", "true")
          .schema(hateSpeechSchema)
          .option("recursiveFileLookup", "true")
          .csv(csvDirectory)

        import spark.implicits._

        // Calculate percentage of hateful messages
        val hatefulMessagesCount = csvDF.filter($"is_hateful" === true).count()
        val totalMessagesCount = csvDF.count()
        val hatefulMessagesPercentage = hatefulMessagesCount / totalMessagesCount * 100

        val frequentHatefulUsers = csvDF.filter($"is_hateful" === 1)
          .groupBy("user")
          .count()
          .orderBy($"count".desc)
          .limit(5)

        val mostRecentMessage = csvDF.orderBy($"timestamp".desc).limit(1)


//        val query = csvDF.writeStream
//          .outputMode("append")
//          .format("console")
//          .start()

        val query = csvDF.writeStream
          .outputMode("append")
          .foreachBatch { (batchDF, batchId) =>
            val hatefulMessagesCount = batchDF.filter($"is_hateful" === true).count()
            val totalMessagesCount = batchDF.count()
            val hatefulMessagesPercentage = if (totalMessagesCount > 0) hatefulMessagesCount.toDouble / totalMessagesCount * 100 else 0.0

            val frequentHatefulUsers = batchDF.filter($"is_hateful" === true)
              .groupBy("user")
              .count()
              .orderBy($"count".desc)
              .limit(5)
              .collect()

            val mostRecentMessage = batchDF.orderBy($"timestamp".desc).limit(1).collect()

            val dataToSend = Map(
              "hatefulMessagesPercentage" -> hatefulMessagesPercentage,
              "frequentHatefulUsers" -> frequentHatefulUsers.map(row => row.getAs[String]("user") -> row.getAs[Long]("count")).toMap,
              "mostRecentMessage" -> mostRecentMessage.map(row => row.getAs[String]("message")).headOption.getOrElse("No message")
            )


            wsClient.send(write(dataToSend))
          }
          .start()

        query.awaitTermination()

       import spark.implicits._

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
