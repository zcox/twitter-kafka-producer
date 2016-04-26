package ch.theza

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{TwitterStreamFactory, RawStreamListener}
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory

object Main extends App with RawStreamListener {

  val config = ConfigFactory.load()
  val log = LoggerFactory.getLogger(getClass)

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)
  val topic = "tweets"

  override def onMessage(message: String): Unit = producer.send(new ProducerRecord[String, String](topic, null, message))
  override def onException(exception: Exception): Unit = {}

  val twitter4jConfiguration = new ConfigurationBuilder()
    .setOAuthConsumerKey(config.getString("twitter.oauth.consumer-key"))
    .setOAuthConsumerSecret(config.getString("twitter.oauth.consumer-secret"))
    .setOAuthAccessToken(config.getString("twitter.oauth.access-token"))
    .setOAuthAccessTokenSecret(config.getString("twitter.oauth.access-token-secret"))
    .build()
  val stream = new TwitterStreamFactory(twitter4jConfiguration).getInstance
  stream.addListener(this)
  stream.sample()
}
