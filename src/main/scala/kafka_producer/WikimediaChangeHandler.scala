package kafka_producer

import com.launchdarkly.eventsource.{EventHandler, MessageEvent}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


class WikimediaChangeHandler(_kafkaProducer: KafkaProducer[String, String], _topic: String) extends EventHandler{

  val kafkaProducer:KafkaProducer[String,String] = _kafkaProducer
  val topic:String = _topic



  override def onOpen(): Unit = None

  override def onClosed(): Unit = kafkaProducer.close()

  override def onMessage(event: String, messageEvent: MessageEvent): Unit = {
    println(messageEvent.getData)
    kafkaProducer.send(new ProducerRecord[String,String](topic,messageEvent.getData))
  }

  override def onComment(comment: String): Unit = None

  override def onError(t: Throwable): Unit = println("error on stream reading ",t)
}
