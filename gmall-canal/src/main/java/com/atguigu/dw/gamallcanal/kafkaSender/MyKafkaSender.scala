package com.atguigu.dw.gamallcanal.kafkaSender

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MyKafkaSender {
  val props = new Properties()
  // Kafka服务端的主机名和端口号
  props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
  // key序列化
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  // value序列化
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)

  def sendToKafka(topic: String, content: String) = {
    producer.send(new ProducerRecord[String, String](topic, content))
  }
}
