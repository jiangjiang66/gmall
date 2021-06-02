package util
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object MyKafkaUtil {
  def getKafkaStream(ssc: StreamingContext, topic: String): DStream[String] = {
    val params: Map[String, String] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil.getProperty("kafka.broker.list"),
      ConsumerConfig.GROUP_ID_CONFIG -> PropertiesUtil.getProperty("kafka.group")
    )
    val value: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, params, Set(topic)).map(_._2)
    value
  }
}
