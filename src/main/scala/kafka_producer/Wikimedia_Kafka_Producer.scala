package kafka_producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import com.launchdarkly.eventsource.EventSource

import java.net.URI
import java.util.Properties
import java.util.concurrent.TimeUnit

object Wikimedia_Kafka_Producer {



  def main(args: Array[String]): Unit = {

    //kafka bootsrap server url
    val bootstrapServers = "127.0.0.1:9092"

    //First step - create the producer properties
    val properties = new Properties()

    //set poperties
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    /*
      //set high throughput producer configs
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)) //32 kib
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    */

    //Second step - create a kafka producer
    val producer = new KafkaProducer[String, String](properties)
      //the kafka topic
    val topic = "wikimedia.recentchange"
      //our data stream url
    val api_url = "https://stream.wikimedia.org/v2/stream/page-create"

    //Third step - create the event source to request http calls to wikimedia data stream
      //event handler interface have the logic to handle with http events, we create our customized event handler
    val eventHandler = new WikimediaChangeHandler(producer,topic)
    val eventSource = new EventSource.Builder(eventHandler,URI.create(api_url)).build()

    //start the producer in another thread
    eventSource.start()

    try TimeUnit.MINUTES.sleep(7)
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }

  }
}
