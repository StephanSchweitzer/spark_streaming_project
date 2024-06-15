import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StringType, IntegerType}
import org.apache.log4j.{Level, Logger}
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake
import java.net.URI
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.collection.mutable

object Consumer {
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  @volatile var wsClient: WebSocketClient = _
  @volatile var isConnected: Boolean = false

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

    // Variable to hold the accumulated DataFrame
    var accumulatedDF: DataFrame = spark.emptyDataFrame

    val csvDF = spark.readStream
      .option("sep", ",")
      .option("header", "true")
      .option("recursiveFileLookup", "true")
      .schema(hateSpeechSchema)
      .csv(csvDirectory)

    import spark.implicits._

    val query = csvDF.writeStream
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // Append the new batch to the accumulated DataFrame
        accumulatedDF = if (accumulatedDF.isEmpty) {
          batchDF
        } else {
          accumulatedDF.union(batchDF)
        }

        // Add batch timestamp and total messages received
        val timestamp = java.time.Instant.now.toString
        val totalMessages = accumulatedDF.count()
        val hatefulMessagesCount = accumulatedDF.filter($"is_hateful" === true).count()
        val hatefulMessagesPercentage = if (totalMessages > 0) hatefulMessagesCount.toDouble / totalMessages * 100 else 0.0

        val frequentHatefulUsers = accumulatedDF.filter($"is_hateful" === true)
          .groupBy("user")
          .count()
          .orderBy($"count".desc)
          .limit(5)
          .collect()

        val mostRecentMessage = accumulatedDF.orderBy($"id".desc).limit(1).collect()

        val dataToSend = Map(
          "batchTimestamp" -> timestamp,
          "totalMessages" -> totalMessages,
          "hatefulMessagesPercentage" -> hatefulMessagesPercentage,
          "frequentHatefulUsers" -> frequentHatefulUsers.map(row => row.getAs[String]("user") -> row.getAs[Long]("count")).toMap,
          "mostRecentMessage" -> mostRecentMessage.map(row => row.getAs[String]("text")).headOption.getOrElse("No message")
        )

        // Print the accumulated data to the terminal
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
