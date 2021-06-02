package sparkTest


import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSparkStreaming {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并初始化SSC
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //2.定义kafka参数
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "topic_event"
    val consumerGroup = "kafkaspark"

    //3.将kafka参数映射为map
    val kafkaParam: Map[String, String] = Map[String, String](
      ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
    )

    //4.通过KafkaUtil创建kafkaDSteam
      val unit: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, Set(topic)).map(_._2)

      unit.print()

    //6.启动SparkStreaming
    ssc.start()
    ssc.awaitTermination()
  }
}
