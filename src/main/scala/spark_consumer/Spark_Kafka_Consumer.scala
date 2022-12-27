package spark_consumer


import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark_Kafka_Consumer {

  val spark = SparkSession.builder()
    .appName("Spark Kafka Consumer")
    .master("local[2]")
    .getOrCreate()


  def readKafka(): Unit ={
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "wikimedia.recentchange")
      .load()

    kafkaDF
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()


  }


  def main(args: Array[String]): Unit = {
    readKafka()
  }

}
