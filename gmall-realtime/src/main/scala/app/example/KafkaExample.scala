package app.example

import java.text.SimpleDateFormat
import java.util.Date

import bean.StartupLog
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{MyKafkaUtil, RedisUtil}
import java.util

import com.wuhui.common.Constant
import com.wuhui.common.util.MyESUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object KafkaExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_STARTUP)

    //sourceStream.print(100)
    //2.把数据封装到样例类中
    val startlogStream: DStream[StartupLog] = sourceStream.map { case x => {
      JSON.parseObject(x, classOf[StartupLog])
    }}
    startlogStream.print(5)

    val firstStartUpStream: DStream[StartupLog] = startlogStream.transform(rdd => {
      //从redis中读取已经启动的设备
      val client: Jedis = RedisUtil.getJedisClient
      var key: String = Constant.TOPIC_STARTUP + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val mids: util.Set[String] = client.smembers(key)
      client.close()
      val midsBd: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(mids)
      //把第一条启动的设备放到redis中，把已经启动的设备过滤，rdd中只保留哪些在redis中不存在的记录
      rdd.filter(log => !midsBd.value.contains(log.uid))
    })

    val filteredStartupLogDStream = firstStartUpStream
      .map(log => (log.uid, log))
      .groupByKey
      .flatMap {
        case (_, logIt) => logIt.toList.sortBy(_.ts).take(1)
      }

    //添加
    filteredStartupLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(startupLogIt => {
        // redis客户端
        val client: Jedis = RedisUtil.getJedisClient
        val startupLogList = startupLogIt.toList
        startupLogList.foreach(startupLog => {
          // 写入到redis的set中
          client.sadd(Constant.TOPIC_STARTUP + ":" + startupLog.logDate, startupLog.uid)
        })
        client.close()
        // 4. 保存到 ES
        MyESUtil.insertBulk(Constant.TOPIC_STARTUP, startupLogList.toIterator)
      })
    })

    // 写入到 Phoenix(HBase)
    import org.apache.phoenix.spark._
    filteredStartupLogDStream.foreachRDD(rdd => {
      rdd.foreach(log => {
        println(log.logType)
      })
      // 参数1: 表名  参数2: 列名组成的 seq 参数 zkUrl: zookeeper 地址
      rdd.saveToPhoenix(
        "GMALL_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
