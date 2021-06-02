package com.atguigu.dw.gamallcanal

import java.sql.DriverManager
import java.util.{Date, Properties}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.dw.gamallcanal.entity.OrderInfo
import com.atguigu.dw.gamallcanal.kafkaSender.MyKafkaSender

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object TempClient {

//  def mock(): Unit ={
//    var consignee =
//    new OrderInfo()
//  }


  def main(args: Array[String]): Unit = {
     //1
        val gsonObj = new JSONObject()
        gsonObj.put("id", 1)
        gsonObj.put("consignee", "lisi")
        gsonObj.put("consignee_tel", "1313255789")
        gsonObj.put("total_amount", "2")
        gsonObj.put("order_status", "1")
        gsonObj.put("user_id", "123")
        gsonObj.put("payment_way", "支付宝")
        gsonObj.put("delivery_address", "天津")
        gsonObj.put("order_comment", "苹果拿大的")
        gsonObj.put("out_trade_no", "111")
        gsonObj.put("trade_body", "红富士")
        gsonObj.put("create_time", new Date)
        gsonObj.put("operate_time", new Date)
        gsonObj.put("expire_time", new Date)
        gsonObj.put("tracking_no", "w111")
        gsonObj.put("parent_order_id", "f111")
        gsonObj.put("img_url", "/img/111.jpg")
        gsonObj.put("province_id","天津")
        MyKafkaSender.sendToKafka("canal",gsonObj.toJSONString)
        Thread.sleep(2000)
        //2
        gsonObj.put("id", 2)
        gsonObj.put("consignee", "jiang")
        gsonObj.put("consignee_tel", "1313255780")
        gsonObj.put("total_amount", "3")
        gsonObj.put("order_status", "2")
        gsonObj.put("user_id", "123")
        gsonObj.put("payment_way", "微信")
        gsonObj.put("delivery_address", "杭州")
        gsonObj.put("order_comment", "小熊绞肉机")
        gsonObj.put("out_trade_no", "222")
        gsonObj.put("trade_body", "小熊")
        gsonObj.put("create_time", new Date)
        gsonObj.put("operate_time", new Date)
        gsonObj.put("expire_time", new Date)
        gsonObj.put("tracking_no", "w222")
        gsonObj.put("parent_order_id", "f222")
        gsonObj.put("img_url", "/img/222.jpg")
        gsonObj.put("province_id","杭州")
        MyKafkaSender.sendToKafka("canal",gsonObj.toJSONString)
  }
}
