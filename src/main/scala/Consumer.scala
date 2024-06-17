import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StringType, IntegerType}
import org.apache.log4j.{Level, Logger}
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake
import java.net.{HttpURLConnection, URL, URI}
import java.io.{BufferedReader, DataOutputStream, InputStreamReader}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.collection.mutable
import java.sql.Timestamp
import java.time.Instant

//COMMENTED TO SHOW HOW
//import com.mongodb.spark.config._
//import com.mongodb.spark.MongoSpark

object Consumer {
  implicit val formats: Formats = DefaultFormats

  val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  @volatile var wsClient: WebSocketClient = _
  @volatile var isConnected: Boolean = false

  case class Message(id: Int, text: String, user: String, is_hateful: Option[Int])
  case class HateSpeechDetectionResponse(id: Int, is_hateful: Int)

  def main(args: Array[String]): Unit = {
    val host = "172.22.134.31" // WSL IP address
    val port = 3001 // WebSocket server port

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    def connectToWebSocket(): Unit = {
      wsClient = new WebSocketClient(new URI(s"ws://$host:$port")) {
        override def onOpen(handshakedata: ServerHandshake): Unit = {
          println("WebSocket connection opened")
          isConnected = true
        }

        override def onMessage(message: String): Unit = {
          println("Received message from server: " + message)
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
      //COMMENTED TO SHOW HOW
      //.config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/messages_db.messages_collection")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val csvDirectory = "./produced_data"
    val checkpointLocation = "./checkpoint"

    // Create the checkpoint directory if it does not exist
    new java.io.File(checkpointLocation).mkdirs()

    val hateSpeechSchema = new StructType()
      .add("id", IntegerType)
      .add("text", StringType)
      .add("user", StringType)
      .add("is_hateful", IntegerType)

    val csvDF = spark.readStream
      .option("sep", ",")
      .option("header", "true")
      .option("recursiveFileLookup", "true")
      .schema(hateSpeechSchema)
      .csv(csvDirectory)

    import spark.implicits._

    // Function to call the hate speech detection API
    def detectHateSpeech(messages: Seq[Message]): Seq[HateSpeechDetectionResponse] = {
      val url = new URL("http://172.22.134.31:3002/detect")
      //val url = new URL("http://90.60.20.92:8000/classify_batch/")
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8")
      connection.setDoOutput(true)

      val cleanedMessages = messages.map(msg =>
        msg.copy(text = msg.text.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "").replaceAll("\u00A0", " "))
      )
      val jsonString = write(cleanedMessages)

      val outputStream = new DataOutputStream(connection.getOutputStream)
      outputStream.write(jsonString.getBytes("UTF-8"))
      outputStream.flush()
      outputStream.close()

      val responseCode = connection.getResponseCode
      if (responseCode == HttpURLConnection.HTTP_OK) {
        val inputStream = new BufferedReader(new InputStreamReader(connection.getInputStream, "UTF-8"))
        val response = new StringBuffer()
        var inputLine = inputStream.readLine()
        while (inputLine != null) {
          response.append(inputLine)
          inputLine = inputStream.readLine()
        }
        inputStream.close()
        org.json4s.jackson.JsonMethods.parse(response.toString).extract[Seq[HateSpeechDetectionResponse]]
      } else {
        println("dead")
        Seq.empty[HateSpeechDetectionResponse]
      }
    }

    // Variables to store accumulated statistics
    var totalHatefulMessages: Long = 0
    var totalRegularMessages: Long = 0
    var userHatefulCounts: mutable.Map[String, Int] = mutable.Map()
    var wordCounts: mutable.Map[String, Int] = mutable.Map()
    var userTotalCounts: mutable.Map[String, Int] = mutable.Map()

    val query = csvDF.writeStream
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // Extract messages and call the hate speech detection API
        val messages = batchDF.as[Message].collect().toSeq
        val detectionResults = detectHateSpeech(messages)

        // Update the DataFrame with the hate speech detection results
        val updatedDF = batchDF.as[Message]
          .map { message =>
            detectionResults.find(_.id == message.id) match {
              case Some(detectionResult) => message.copy(is_hateful = Some(detectionResult.is_hateful))
              case None => message
            }
          }
          .toDF()

        //COMMENTED TO SHOW HOW
        // Save updatedDF to MongoDB
        //        updatedDF.write
        //          .format("mongodb")
        //          .mode("append")
        //          .option("database", "messages_db")
        //          .option("collection", "messages_collection")
        //          .save()

        // Update total message counts
        totalHatefulMessages += updatedDF.filter(col("is_hateful") === 1).count()
        totalRegularMessages += updatedDF.filter(col("is_hateful") === 0).count()

        // Update user hateful counts
        updatedDF.groupBy("user").agg(
          sum("is_hateful").as("hateful_count"),
          count("id").as("total_count")
        ).collect().foreach(row => {
          val user = row.getString(0)
          val hatefulCount = row.getLong(1).toInt
          val totalCount = row.getLong(2).toInt
          userHatefulCounts(user) = userHatefulCounts.getOrElse(user, 0) + hatefulCount
          userTotalCounts(user) = userTotalCounts.getOrElse(user, 0) + totalCount
        })

        val top5Users = userHatefulCounts.toSeq.sortBy(-_._2).take(5).map { case (user, count) => Map("user" -> user, "count" -> count) }

        val top5UsersByTotal = userTotalCounts.toSeq.sortBy(-_._2).take(5).map { case (user, count) => Map("user" -> user, "count" -> count) }

        val totalMessages = totalHatefulMessages + totalRegularMessages

        val hateSpeechRatio = if (totalMessages > 0) {
          totalHatefulMessages.toDouble / totalMessages
        } else {
          0.0
        }

        val unwantedWords = Set("je", "tu", "il", "elle", "nous", "vous", "ils", "elles", "le", "la", "les", "un", "une", "des", "sommes", "est",
        "ont", "ai", "are", "c'est", "it's", "is", "was", "by", "en", "sa", "son", "@url", "ne", "not", "pas",
        "that", "a", "n'est", "to", "par", "de", "ce", "sont", "a", "them", "it", "aux", "you", "avec", "in", "dans",
          "@user", "et", "que", "à", "of", "qui", "the", "and", "du", "sur", "si", "if", "au", "aux", "pour", "mais",
          "for", "but", "plus", "suis", "se", "«", "they", "comme", "have", "ou", "?", "quand", "ça", "fait", "c")

        updatedDF.select("text").as[String].collect()
          .flatMap(_.split("\\s+"))
          .filterNot(word => unwantedWords.contains(word.toLowerCase))
          .foreach(word => {
            wordCounts(word) = wordCounts.getOrElse(word, 0) + 1
          })

        val top10Words = wordCounts.toSeq.sortBy(-_._2).take(10).map { case (word, count) => Map("word" -> word, "count" -> count) }

        // Prepare messages to be sent to the WebSocket server
        val messagesToSend = updatedDF.as[Message].collect().toSeq


        val dataToSend = Map(
          "messages" -> messagesToSend,
          "batchSize" -> messagesToSend.size,
          "hatefulBatchSize" -> updatedDF.filter(col("is_hateful") === 1).count(),
          "timestamp" -> Timestamp.from(Instant.now()).toString,
          "totalHatefulMessages" -> totalHatefulMessages,
          "totalRegularMessages" -> totalRegularMessages,
          "totalMessages" -> totalMessages,
          "hateSpeechRatio" -> hateSpeechRatio,
          "top5Users" -> top5Users,
          "top10Words" -> top10Words,
          "top5ActiveUsers" -> top5UsersByTotal
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
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}
