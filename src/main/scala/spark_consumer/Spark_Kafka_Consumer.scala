package spark_consumer


import models._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.{Json, parseJson}

object Spark_Kafka_Consumer {



  val spark = SparkSession.builder()
    .appName("Spark Kafka Consumer")
    .master("local[2]")
    .getOrCreate()

  //import spark.implicits._

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams:Map[String,Object]=Map(
    "bootstrap.servers" -> "127.0.0.1:9092",
    "key.serializer" -> classOf[StringSerializer], // send data to kafka
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer], // receiving data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  val kafkaTopic = "wikimedia.recentchange"

  def readFromKafka() = {
    val topics = Array(kafkaTopic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      /*
       Distributes the partitions evenly across the Spark cluster.
       Alternatives:
       - PreferBrokers if the brokers and executors are in the same cluster
       - PreferFixed
      */
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
      /*
        Alternative
        - SubscribePattern allows subscribing to topics matching a pattern
        - Assign - advanced; allows specifying offsets and partitions per topic
       */
    )




    val processedStream = kafkaDStream.map(record =>(record.key(),record.value()))
    //processedStream.print()

    val atributesStream = processedStream.map{line=>

      val jsonString= s"""${line._2}"""
      val jsonValues = parseJson(jsonString)

      val domain = jsonValues\"meta"\"domain"
      val uri = jsonValues\"meta"\"uri"
      val pageTitle = jsonValues\"page_title"
      val revLen = jsonValues\"rev_len"
      val revDate = jsonValues\"rev_timestamp"

      new WikimediaNewArticle(domain.toString,
                              uri.toString,
                              pageTitle.toString,
                              revLen.toString,
                              revDate.toString
      )

    }


    atributesStream.print()

    ssc.start()
    ssc.awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    //readKafka()
    readFromKafka()
  }

}
