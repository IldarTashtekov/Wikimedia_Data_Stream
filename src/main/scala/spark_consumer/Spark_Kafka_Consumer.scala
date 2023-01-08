package spark_consumer


import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.{Json, parseJson}

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.reflect.io.File

object Spark_Kafka_Consumer {



  val spark: SparkSession = SparkSession.builder()
    .appName("Spark Kafka Consumer")
    .master("local[2]")
    .getOrCreate()


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

  var dom_map:scala.collection.mutable.Map[String,Int]=mutable.Map()

  val domMapSchema: StructType =StructType(Array(
    StructField("Domain", StringType),
    StructField("Number_of_appearances ", IntegerType)
  ))

  val csvPath="src/main/scala/spark_consumer/domains_csv"

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

    val atributesStream = processedStream.map{line=>

      val jsonString= s"""${line._2}"""
      val jsonValues = parseJson(jsonString)

      val domain = jsonValues\"meta"\"domain"
      /*val uri = jsonValues\"meta"\"uri"
      val pageTitle = jsonValues\"page_title"
      val revLen = jsonValues\"rev_len"
      val revDate = jsonValues\"rev_timestamp"

      new WikimediaNewArticle(domain.values.toString,
                              uri.values.toString,
                              pageTitle.values.toString,
                              revLen.values.toString,
                              revDate.values.toString
      )*/

      domain.values.toString
    }

    atributesStream.foreachRDD{ rdd =>
      rdd.foreach{ dom =>

          if(dom_map.keys.exists(_==dom)){

            dom_map(dom)+=1

          }else{

            dom_map.put(dom,1)

          }

        println(dom_map)

     }
    }

    ssc.start()


    try TimeUnit.MINUTES.sleep(1)
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }

    ssc.stop(false)

  }

  def loadCSV(): Unit ={
    if(File(csvPath).exists){
      val loadCSV_DF = spark.read
        .schema(domMapSchema)
        .csv(csvPath)

      loadCSV_DF.show()

      loadCSV_DF.foreach{row=>

        val domain:String = row(0).asInstanceOf[String]
        val value:Int = row(1).asInstanceOf[Int]
        dom_map.put(domain, value)

        //explicitly returns Unit
        ()

      }

      dom_map.foreach(print)

    }else{
      println("CSV dont exist, probably this is the first run, if it is not something dont work well")
    }


  }

  def saveCSV(): Unit ={

    val rows_seq = dom_map.map(dom=>Row(dom._1,dom._2)).toSeq
    val rowRDDs = spark.sparkContext.parallelize(rows_seq)
    val saveCSV_DF = spark.createDataFrame(rowRDDs,domMapSchema)
    saveCSV_DF.write.mode("overwrite").csv(csvPath)
  }


  def main(args: Array[String]): Unit = {

    loadCSV()
    readFromKafka()
    saveCSV()

  }

}
