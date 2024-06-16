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

object Consumer {
  implicit val formats: Formats = DefaultFormats

  val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  @volatile var wsClient: WebSocketClient = _
  @volatile var isConnected: Boolean = false

  case class Message(id: Int, text: String, user: String, is_hateful: Option[Int])
  case class HateSpeechDetectionResponse(id: Int, is_hateful: Int)

  def main(args: Array[String]): Unit = {
    val host = "172.22.134.31" // Use the WSL IP address here
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
        Seq.empty[HateSpeechDetectionResponse]
      }
    }

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

        // Prepare messages to be sent to the WebSocket server
        val messagesToSend = updatedDF.as[Message].collect().toSeq

        val dataToSend = Map(
          "messages" -> messagesToSend
        )

        // Print the batch data to the terminal
        println(s"Batch $batchId data: $dataToSend")

        if (isConnected) {
          try {
            // Serialize data to JSON string
            val jsonData = write(dataToSend)
            wsClient.send(jsonData)
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