package spark_consumer


import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.{Json, parseJson}

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.reflect.io.File

object Spark_Kafka_Consumer {


  //spark session
  val spark: SparkSession = SparkSession.builder()
    .appName("Spark Kafka Consumer")
    .master("local[2]")
    .getOrCreate()

  //streaming context to use spark streaming
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  //kafka params
  val kafkaParams:Map[String,Object]=Map(
    "bootstrap.servers" -> "127.0.0.1:9092",
    "key.serializer" -> classOf[StringSerializer], // send data to kafka
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer], // receiving data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  //kafka topic to consume on the same kafka topic where the kafka producer is sending data
  val kafkaTopic = "wikimedia.recentchange"

  //a map when we save the domain and the number of apperances of that domain
  var dom_map:scala.collection.mutable.Map[String,Int]=mutable.Map()

  //a schema to create a df from rdds and save in csv later
  val domMapSchema: StructType =StructType(Array(
    StructField("Domain", StringType),
    StructField("Number_of_appearances ", IntegerType)
  ))

  //the path of the csv when we gonna save the domain and its number of appearances
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

    //raw kafka stream, receive a integer key and string value, the value is a json with information about new articles in wikimedia
    val processedStream = kafkaDStream.map(record =>(record.key(),record.value()))

    //we only need the domain, for this reason we gonna do some transformations
    val atributesStream = processedStream.map{line=>

      //first we gonnna transform a string with json in json4s.JValue object to get data in more easy way
      val jsonString= s"""${line._2}"""
      val jsonValues = parseJson(jsonString)

      //then get the value we searching
      val domain = jsonValues\"meta"\"domain"

      //every iteration returns a string with the domain name
      domain.values.toString
    }

    //we gonna iterate the stream with domains
    //RDD[String]
    atributesStream.foreachRDD{ rdd =>
      //String
      rdd.foreach{ dom =>

          //if domain exist then we sum 1 to value if not we add the domain with value 1
          if(dom_map.keys.exists(_==dom)){

            dom_map(dom)+=1

          }else{

            dom_map.put(dom,1)

          }

        println(dom_map)

     }
    }

    //the stream starts
    ssc.start()


    //the stream gonna be active for 1 minute
    try TimeUnit.MINUTES.sleep(1)
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
    //stream context stops but spark context still active becouse we gonna use it to save
    //the map information to csv
    ssc.stop(false)

  }


  def saveCSV(): Unit ={

    //Map[String, Int] => Seq[Row[String,Int]]
    val rows_seq = dom_map.map(dom=>Row(dom._1,dom._2)).toSeq
    //Seq[Row] => RDD[Row]
    val rowRDDs = spark.sparkContext.parallelize(rows_seq)
    //RDD[Row] => DataFrame
    val saveCSV_DF = spark.createDataFrame(rowRDDs,domMapSchema)
    //show the DF in terminal
    saveCSV_DF.show()
    //here we save the dataframe to csv
    saveCSV_DF.write.mode("overwrite").csv(csvPath)
  }


  def loadCSV(): Unit ={

    //we load the csv as DF
      val loadCSV_DF = spark.read
        .schema(domMapSchema)
        .csv(csvPath)

      loadCSV_DF.show()

      //we get the data of every row and put it inside the map
      loadCSV_DF.foreach{row=>

        val domain:String = row(0).asInstanceOf[String]
        val value:Int = row(1).asInstanceOf[Int]
        dom_map.put(domain, value)

        //explicitly returns Unit
        ()
      }

      dom_map.foreach(print)
  }

  def main(args: Array[String]): Unit = {

    //is csv file exists we load the csv
    if(File(csvPath).exists) loadCSV()

    //our kafka streaming when we compile the data about domain appearances
    readFromKafka()

    //here we save the csv to some data persistence
    saveCSV()

  }

}
