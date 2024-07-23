import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.log4j.{Level, Logger}
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake
import org.apache.spark.sql.execution.streaming.MemoryStream

import java.net.{HttpURLConnection, URI, URL}
import java.io.{BufferedReader, DataOutputStream, InputStreamReader, OutputStreamWriter}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.jackson.JsonMethods._

import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ScheduledExecutorService, TimeUnit}
import scala.collection.mutable
import scala.collection.mutable.Queue
import java.sql.Timestamp
import java.time.Instant

case class Message(`type`: String, id: String, text: String, user: String, is_hateful: Option[Int])
case class HateSpeechDetectionResponse(id: String, is_hateful: Int)
case class UserAggregations(user: String, hateful_count: Long, total_count: Long)
case class WordCount(word: String, count: Long)

object HateSpeechDetector {
  implicit val formats = DefaultFormats

  def detectHateSpeech(messagesIterator: Iterator[Message], chunkSize: Int): Iterator[HateSpeechDetectionResponse] = {
    val url = new URL("http://172.22.134.31:3002/detect")
    //val url = new URL("https://deepwoke.com/predict/classify_batch")
    messagesIterator.grouped(chunkSize).flatMap { messagesChunk =>
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8")
      connection.setDoOutput(true)

      val cleanedMessages = messagesChunk.map { msg =>
        msg.copy(text = msg.text.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "").replaceAll("\u00A0", " "))
      }

      // Convert cleanedMessages to JSON
      val messagesJson = write(cleanedMessages)

      // Send request
      val outputStream = connection.getOutputStream
      val writer = new OutputStreamWriter(outputStream, "UTF-8")
      writer.write(messagesJson)
      writer.flush()
      writer.close()

      // Get response code
      val responseCode = connection.getResponseCode

      if (responseCode == HttpURLConnection.HTTP_OK) {
        val responseStream = connection.getInputStream
        val responseJson = scala.io.Source.fromInputStream(responseStream).mkString
        responseStream.close()

        parse(responseJson).extract[Seq[HateSpeechDetectionResponse]].iterator
      } else {
        println("Error response from server: " + responseCode)
        messagesChunk.map(msg => HateSpeechDetectionResponse(msg.id, 0)).iterator // Default responses in case of error
      }
    }
  }
}


object Consumer {
  implicit val formats: Formats = DefaultFormats

  val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  @volatile var wsClient: WebSocketClient = _
  @volatile var isConnected: Boolean = false


  val receivedMessages: ConcurrentLinkedQueue[Message] = new ConcurrentLinkedQueue[Message]()
  implicit val messageEncoder: Encoder[Message] = Encoders.product[Message]

  def main(args: Array[String]): Unit = {
    val host = "172.22.134.31" // WSL IP address
    val port = 3001 // WebSocket server port

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    def connectToWebSocket(): Unit = {
      wsClient = new WebSocketClient(new URI(s"ws://$host:$port?clientType=spark")) {
        override def onOpen(handshakedata: ServerHandshake): Unit = {
          println("WebSocket connection opened")
          isConnected = true
        }

        override def onMessage(message: String): Unit = {
          val receivedMessage = parse(message).extract[Message]
          println(receivedMessage)
          if (receivedMessage.`type` == "inbound") {
            receivedMessages.add(receivedMessage)
          }
        }

        override def onClose(code: Int, reason: String, remote: Boolean): Unit = {
          println(s"WebSocket connection closed: $reason")
          isConnected = false
          scheduleReconnect()
        }

        override def onError(ex: Exception): Unit = {
          println("WebSocket error observed: ")
          ex.printStackTrace()
        }
      }
      wsClient.connect()
    }

    def scheduleReconnect(): Unit = {
      scheduler.schedule(new Runnable {
        override def run(): Unit = {
          connectToWebSocket()
        }
      }, 5, TimeUnit.SECONDS)
    }

    connectToWebSocket()

    val spark = SparkSession.builder()
      .appName("Consumer")
      .master("local[*]")
      //      .config("spark.mongodb.input.collection", "messages")
      //      .config("spark.mongodb.output.collection", "messages")
      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val csvDirectory = "./produced_data"
    //val checkpointLocation = "./checkpoint"

    // Create the checkpoint directory if it does not exist
    //new java.io.File(checkpointLocation).mkdirs()

    val hateSpeechSchema = new StructType()
      .add("type", StringType)
      .add("id", StringType) // Ensure id is String to match the case class
      .add("text", StringType)
      .add("user", StringType)
      .add("is_hateful", IntegerType)

    val csvDF = spark.readStream
      .option("sep", ",")
      .option("header", "true")
      .option("recursiveFileLookup", "true")
      .schema(hateSpeechSchema)
      .csv(csvDirectory)

    val memoryStream = MemoryStream[Message](1, spark.sqlContext)
    val wsDF = memoryStream.toDF()

    // Periodically poll the queue and add messages to the MemoryStream
    new Thread(new Runnable {
      def run(): Unit = {
        while (true) {
          if (!receivedMessages.isEmpty) {
            val messages = new mutable.Queue[Message]()
            while (!receivedMessages.isEmpty) {
              messages += receivedMessages.poll()
            }
            memoryStream.addData(messages)
          }
          //Thread.sleep(1000) // Adjust the sleep time as needed
        }
      }
    }).start()

    val unifiedDF = csvDF.unionByName(wsDF)

    import spark.implicits._


    // Variables to store accumulated statistics
    var userAggregationsDF: DataFrame = spark.emptyDataset[UserAggregations].toDF()
    var wordCountDF: DataFrame = spark.emptyDataset[WordCount].toDF()


    val query = unifiedDF.writeStream
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val messagesDS: Dataset[Message] = batchDF.as[Message]
        val chunkSize = 1000

        val detectionResultsDS = messagesDS.mapPartitions { messagesIterator =>
          HateSpeechDetector.detectHateSpeech(messagesIterator, chunkSize)
        }(Encoders.product[HateSpeechDetectionResponse]).toDF()

        val detectionResults = detectionResultsDS.withColumnRenamed("is_hateful", "detected_is_hateful")
        val updatedDF = messagesDS.toDF()
          .join(detectionResults, Seq("id"), "left_outer")
          .withColumn("is_hateful", coalesce(col("detected_is_hateful"), col("is_hateful")))

        updatedDF.select("id", "is_hateful", "text", "user")
          .write
          .format("mongodb")
          .mode("append")
          .option("uri", "mongodb://localhost:27017")
          .option("spark.mongodb.database", "messages")
          .option("spark.mongodb.collection", "messages")
          .save()

        val userAggBatchDF = updatedDF.groupBy("user")
          .agg(
            sum(when(col("is_hateful") === 1, 1).otherwise(0)).as("hateful_count"),
            count("id").as("total_count")
          )
          .as[UserAggregations]
          .toDF()

        val unwantedWords: Set[String] = Set("je", "tu", "il", "elle", "nous", "vous", "ils", "elles", "le", "la", "les", "un", "une", "des", "sommes", "est",
          "ont", "ai", "are", "c'est", "it's", "is", "was", "by", "en", "sa", "son", "@url", "ne", "not", "pas",
          "that", "a", "n'est", "to", "par", "de", "ce", "sont", "a", "them", "it", "aux", "you", "avec", "in", "dans",
          "@user", "et", "que", "à", "of", "qui", "the", "and", "du", "sur", "si", "if", "au", "aux", "pour", "mais",
          "for", "but", "plus", "suis", "se", "«", "they", "comme", "have", "ou", "?", "quand", "ça", "fait", "c", "tout", "contre")


        val wordCountBatchDF = updatedDF.select("text").as[String]
          .flatMap(_.split("\\s+"))
          .filter(word => !unwantedWords.contains(word.toLowerCase))
          .groupBy("value")
          .count()
          .select($"value".as("word"), $"count".as("count"))
          .as[WordCount]
          .toDF()

        userAggregationsDF = userAggregationsDF.union(userAggBatchDF)
          .groupBy("user")
          .agg(
            sum("hateful_count").as("hateful_count"),
            sum("total_count").as("total_count")
          )
          .as[UserAggregations]
          .toDF()

        wordCountDF = wordCountDF.union(wordCountBatchDF)
          .groupBy("word")
          .agg(sum("count").as("count"))
          .as[WordCount]
          .toDF()

        val top5Users = userAggregationsDF.orderBy(desc("hateful_count")).limit(5)
        val top10Words = wordCountDF.orderBy(desc("count")).limit(10)


//        // Update user hateful counts
//        updatedDF.groupBy("user").agg(
//          sum("is_hateful").as("hateful_count"),
//          count("id").as("total_count")
//        ).collect().foreach(row => {
//          val user = row.getString(0)
//          val hatefulCount = row.getLong(1).toInt
//          val totalCount = row.getLong(2).toInt
//          userHatefulCounts(user) = userHatefulCounts.getOrElse(user, 0) + hatefulCount
//          userTotalCounts(user) = userTotalCounts.getOrElse(user, 0) + totalCount
//        })

//        val totalMessages = totalHatefulMessages + totalRegularMessages
//        val hateSpeechRatio = if (totalMessages > 0) totalHatefulMessages.toDouble / totalMessages else 0.0
//
//        updatedDF.select("text").as[String].collect()
//          .flatMap(_.split("\\s+"))
//          .filterNot(word => unwantedWords.contains(word.toLowerCase))
//          .foreach(word => wordCounts(word) = wordCounts.getOrElse(word, 0) + 1)

        // Prepare messages to be sent to the WebSocket server
        //val messagesToSend = updatedDF.as[Message].collect().toSeq

        // Prepare data to send to WebSocket
        val dataToSend = Map(
          "batchSize" -> updatedDF.count(),
          "hatefulBatchSize" -> updatedDF.filter(col("is_hateful") === 1).count(),
          "timestamp" -> Timestamp.from(Instant.now()).toString,
          "totalHatefulMessages" -> userAggregationsDF.agg(sum("hateful_count")).as[Long].first(),
          "totalRegularMessages" -> (userAggregationsDF.agg(sum("total_count")).as[Long].first() - userAggregationsDF.agg(sum("hateful_count")).as[Long].first()),
          "totalMessages" -> userAggregationsDF.agg(sum("total_count")).as[Long].first(),
          "hateSpeechRatio" -> (userAggregationsDF.agg(sum("hateful_count")).as[Long].first().toDouble / userAggregationsDF.agg(sum("total_count")).as[Long].first()),
          "top5Users" -> top5Users.as[UserAggregations].collect().map { case UserAggregations(user, hateful_count, total_count) => Map("user" -> user, "hateful_count" -> hateful_count, "total_count" -> total_count) },
          "top10Words" -> top10Words.as[WordCount].collect().map { case WordCount(word, count) => Map("word" -> word, "count" -> count) }
        )

        if (isConnected) {
          try {
            val jsonData = write(dataToSend)
            wsClient.send(jsonData)
            println("sent data")
          } catch {
            case e: Exception => println("Failed to send batch data: " + e.getMessage)
          }
        } else {
          println("WebSocket is not connected. Data not sent.")
        }
      }
      //.option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}