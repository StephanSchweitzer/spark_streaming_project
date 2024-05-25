import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count

import java.io.File



object Producer {
  def main(args: Array[String]): Unit = {

    //to make the project run on windows you need this folder with winutils.ext and hadoop.dll to be linked
    System.setProperty("hadoop.home.dir", "resources/hadoop")

    val logFile = "source_data/all_data.csv"

    val spark = SparkSession.builder
      .appName("Producer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val options = Map("delimiter"->",","header"->"true")
    var logData = spark.read.options(options).csv(logFile).persist()

    println("number of lines in my df " + logData.count())
    val number_of_partitions: Int = (logData.count()/2000).toInt
    println("number of partitions =" + number_of_partitions)

    // println(number_of_partitions)

    for (i <- 0 to number_of_partitions)
    {
      val to_write = logData.limit(2000)
      // Écrire le DataFrame actuel au format CSV
      println(s"writing to produced_data/partition_${i}.csv")
      to_write.write
        .format("csv")
        .options(options)
        .mode("overwrite")
        .save(s"produced_data/partition_${i}.csv")

      // Supprimer les 1152 premières lignes du DataFrame
      logData = logData.except(to_write)
      to_write.unpersist()
      println("remain lines in df " + logData.count())
      // Thread.sleep(5000)
    }
  }
}