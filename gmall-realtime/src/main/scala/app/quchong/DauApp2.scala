package app.quchong

import java.text.SimpleDateFormat
import java.util.Date

import bean.StartupLog
import com.alibaba.fastjson.JSON
import com.wuhui.common.Constant
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import util.{MyKafkaUtil, RedisUtil}
import java.util

import org.apache.spark.broadcast.Broadcast

object DauApp2 {
  def main(args: Array[String]): Unit = {
    //1.从kafka消费数据
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val sourceStream: DStream[String] =
                        MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_STARTUP)
    //2.把数据封装到样例类中
    val startlogStream: DStream[StartupLog] = sourceStream.map { case x => {
        val log: StartupLog = JSON.parseObject(x, classOf[StartupLog])
        // 给 log 的另外两个字段赋值: logDate logHour
        val date = new Date(log.ts)
        log.logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
        log.logHour = new SimpleDateFormat("HH").format(date)
        log
      }
    }
//    startlogStream.print(1000)
    //3.把已经启动的设备id放到redis中，用set集合，就可以只保留一个
    //对启动记录过滤，已经启动过（redis中有记录）的不写到hbase中
    //把已经启动
    var firstStartUpStream: DStream[StartupLog] = startlogStream.transform(rdd => {
      //从redis中读取已经启动的设备
      val client: Jedis = RedisUtil.getJedisClient
      var key: String = Constant.TOPIC_STARTUP + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val mids: util.Set[String] = client.smembers(key)
      val midsBd: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(mids)
      client.close()
      //把第一条启动的设备放到redis中，把已经启动的设备过滤，rdd中只保留哪些在redis中不存在的记录
      rdd.filter(log => {
        val uids: util.Set[String] = midsBd.value
        !uids.contains(log.mid)
      })
    })
    //添加
    // 2.3 批次内去重:  如果一个批次内, 一个设备多次启动(对这个设备来说是第一个批次), 则前面的没有完成去重
    firstStartUpStream = firstStartUpStream
      .map(log => (log.mid, log))
      .groupByKey
      .flatMap {
        case (_, logIt) => logIt.toList.sortBy(_.ts).take(1)
      }
    import org.apache.phoenix.spark._
    // 2.4 写入到redis，用处是为了去重
    firstStartUpStream.foreachRDD(rdd => {
      rdd.foreachPartition(startupLogIt => {
        // redis客户端
        val client: Jedis = RedisUtil.getJedisClient
        val startupLogList = startupLogIt.toList
        startupLogList.foreach(startupLog => {
          // 写入到redis的set中
          client.sadd( Constant.TOPIC_STARTUP+ ":" + startupLog.logDate, startupLog.mid)
        })
        client.close()
      })
      //写到hbase
      rdd.saveToPhoenix(Constant.DAU_TABLE,
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    firstStartUpStream.print(1000)

    //写到hbase

    // 1. 调整数据结构
//    val starupLogDSteam = sourceStream.map {
//      case (_, log) => JSON.parseObject(log, classOf[StartupLog])
//    }
    // 2. 保存到 redis
//    starupLogDSteam.foreachRDD(rdd => {
//      rdd.foreachPartition(it => {
//        val client: Jedis = RedisUtil.getJedisClient
//        it.foreach(startupLog => {
//          // 存入到 Redis value 类型 set, 存储 uid
//          val key = "dau:" + startupLog.logDate
//          client.sadd(key, startupLog.uid)
//        })
//        client.close()
//      })
//    })
    ssc.start()
    ssc.awaitTermination()
  }
}
/*
 把已经启动的设备id放到redis中，用set集合，就可以只保留一个
   set
   key  value
 对启动记录过滤，已经启动过（redis中有记录）的不写到hbase中
 */