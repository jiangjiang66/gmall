package app

import java.text.SimpleDateFormat

import util.MyEsKafkaUtil
import bean.{AlertInfo, EventLog}
import com.alibaba.fastjson.JSON
import java.util
import java.util.Date

import com.wuhui.common.Constant
import com.wuhui.common.util.MyESUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._
object AlertApp {
  def main(args: Array[String]): Unit = {
    // 1. 从kafka消费数据(事件日志)
    val conf: SparkConf = new SparkConf().setAppName("DAUApp").setMaster("local[1]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //获取kafka内容
    val sourceDStream: InputDStream[(String, String)] = MyEsKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_EVENT)
   sourceDStream.print()
    // 2. 添加窗口
    val windowStream: DStream[(String, String)] = sourceDStream.window(Seconds(5*60),Seconds(5))
    //3.,调整数据结构
    val eventLog: DStream[(String, EventLog)] = windowStream.map {
      case (_, jsonString) => {
        val log = JSON.parseObject(jsonString, classOf[EventLog])
        val date = new Date(log.ts)
        log.logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
        log.logHour = new SimpleDateFormat("HH").format(date)
        (log.mid, log)
      }
    }
//    eventLog.print()
    //4.按照 uid 分组
    val groupedEventLogDStream: DStream[(String, Iterable[EventLog])] = eventLog.groupByKey()
    //预警的业务逻辑
    val checkCouponAlertDStream: DStream[(Boolean, AlertInfo)] = groupedEventLogDStream.map {
      case (mid, logIt) => {
        val uids: util.HashSet[String] = new util.HashSet[String]() //领取优惠券的用户id
        val itemIds: util.HashSet[String] = new util.HashSet[String]() //领取优惠券的商品id
        val eventIds: util.ArrayList[String] = new util.ArrayList[String]() //时间id
        var isBrowserProduct: Boolean = false // 是否浏览商品, 默认没有浏览
        breakable {
          logIt.foreach(log => {
            eventIds.add(log.eventId)
            //  记录下领优惠全的所有用户
            if (log.eventId == "coupon") {
              uids.add(log.uid) // 领优惠券的用户id
              itemIds.add(log.itemId) // 用户领券的商品id
            } else {
              isBrowserProduct = true
              break   //预览商品了，可能领取大于三个优惠券，但是因为中间浏览商品了，所以这个mid还可以继续领取优惠券了
            }
          })
        }
        // 组合成元组  (是否预警, 预警信息)
        (!isBrowserProduct && uids.size() >= 3, AlertInfo(mid, uids, itemIds, eventIds, System.currentTimeMillis()))
      }
    }

    //过滤掉不需要报警的信息
    val filteredDStream: DStream[AlertInfo] = checkCouponAlertDStream.filter(_._1).map(_._2)
    val alertInfoWithIdDStream = filteredDStream.map(alertInfo => (alertInfo.mid + "_" + alertInfo.ts / 1000 / 60, alertInfo))

    // 6.2 写入到es中
    alertInfoWithIdDStream.foreachRDD(rdd => {
      rdd.foreachPartition(it => MyESUtil.insertBulk("gmall_coupon_alert", it))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
