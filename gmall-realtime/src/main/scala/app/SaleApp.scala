package app

import java.util.Properties

import bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.alibaba.fastjson.JSON
import com.wuhui.common.Constant
import com.wuhui.common.util.MyESUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis
import util.{MyKafkaUtil, RedisUtil}

object SaleApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RealtimeApp").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    // 1. 读取连个流, 并封装数据
    val orderInfoStream: DStream[(String, OrderInfo)] = MyKafkaUtil
      .getKafkaStream(ssc, Constant.TOPIC_ORDER)
      .map {
        case x =>
          val orderInfo: OrderInfo = JSON.parseObject(x, classOf[OrderInfo])
          (orderInfo.id, orderInfo)
      }
    val orderDetailStream: DStream[(String, OrderDetail)] = MyKafkaUtil
      .getKafkaStream(ssc, Constant.TOPIC_ORDER_DETAIL)
      .map {
        case x =>
          val orderDetail: OrderDetail = JSON.parseObject(x, classOf[OrderDetail])
          (orderDetail.order_id, orderDetail)
      }
    // 2. 双流 join
    val fullJoinStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoStream.fullOuterJoin(orderDetailStream)
    // 3. 写缓存和读缓存
    val saleDetailStreamWithoutUserInfo: DStream[SaleDetail] = fullJoinStream.mapPartitions((it: Iterator[(String, (Option[OrderInfo], Option[OrderDetail]))]) => {
      val readClient: Jedis = RedisUtil.getJedisClient
      val writeClient: Jedis = RedisUtil.getJedisClient
      val result: Iterator[SaleDetail] = it.flatMap {
        case (orderId, (Some(orderInfo), Some(orderDetail))) => // 组成 SaleDetail, 并缓存数据

          /*
          由于 order_info 与 order_detail 是一对多的关系,
          后面的时间段会有可能用到 order_info 的数据, 所以需要把 order_info 的数据做缓存
           */
          val orderInfoKey = s"order_info_${orderInfo.id}"
          println("orderInfo:orderDetail: " + orderInfo)
          cacheToRedis(writeClient, orderInfoKey, orderInfo)
          Array(SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail))

        case (orderId, (Some(orderInfo), None)) => // 从缓存读数据, 并把 orderInfo 缓存到数据
          // 缓存 order_info
          val orderInfoKey = s"order_info_${orderInfo.id}"
          cacheToRedis(writeClient, orderInfoKey, orderInfo)

          import collection.JavaConversions._
          import java.util

          // 读取 OrderDetail 缓存
          val orderDetailList: List[OrderDetail] = readClient.keys("order_detail_*")
            .toList
            .map(key => {
              val value: String = readClient.get(key)
              println("value: " + value)
              JSON.parseObject(value, classOf[OrderDetail])
            })

          orderDetailList.filter(_.order_id == orderId).map(orderDetail => {
            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
          })
        case (orderId, (None, Some(orderDetail))) => // 从缓存读珊瑚橘, 并把 orderDetail 缓存数据

          // 缓存 order_detail
          val orderDetailKey = s"order_detail_${orderDetail.id}"
          cacheToRedis(writeClient, orderDetailKey, orderDetail)

          // 读取 OrderInfo
          val orderInfoJson: String = readClient.get(s"order_info_${orderDetail.order_id}")
          if (orderInfoJson != null && orderInfoJson.startsWith("{")) { // 如果缓存存在
            println("orderInfojson: " + orderInfoJson)
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            Array(SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail))
          } else { // 如果不存在则返回一个空集合
            Array[SaleDetail]()
          }
      }
      writeClient.close()
      readClient.close()
      result
    })

    // 4. 查询 Mysql 管理 UserInfo

    val spark: SparkSession = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
    import spark.implicits._
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "aaaaaa")
    val finalStream: DStream[SaleDetail] = saleDetailStreamWithoutUserInfo.transform(rdd => {
      val userInfoRDD = spark.read.jdbc("jdbc:mysql://hadoop102:3306/gmall0722", "user_info", props)
        .as[UserInfo]
        .rdd
        .map(userInfo => (userInfo.id, userInfo))

      val joinedRDD: RDD[(String, (SaleDetail, UserInfo))] = rdd.map(saleDetail => (saleDetail.user_id, saleDetail)).join(userInfoRDD)

      // 把 userInfo 的信息补齐
      joinedRDD.map {
        case (_, (saleDetail, userInfo)) => saleDetail.mergeUserInfo(userInfo)
      }
    })

    finalStream.print(1000)

    finalStream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        MyESUtil.insertBulk(Constant.INDEX_SALE_DETAIL, it)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 写入缓存
    *
    * @param client
    * @param key
    * @param value
    */
  def cacheToRedis[T <: AnyRef](client: Jedis, key: String, value: T): Unit = {
    val jsonString: String = Serialization.write(value)(DefaultFormats)
    client.setex(key, 1000 * 60 * 30, jsonString)
  }


}
