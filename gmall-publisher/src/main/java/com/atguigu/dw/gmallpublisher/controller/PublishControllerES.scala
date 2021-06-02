package com.atguigu.dw.gmallpublisher.controller

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date

import com.atguigu.dw.gmallpublisher.bean.{Opt, SaleInfo, Stat}
import com.atguigu.dw.gmallpublisher.service.PublishServiceESImpl
import org.apache.commons.lang.time.DateUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{GetMapping, RequestParam}

import scala.collection.mutable

@Controller
class PublishControllerES {
  @Autowired
  private var publisherService : PublishServiceESImpl = _

  @GetMapping(Array("realtime-total"))
  def getDauTotal(@RequestParam("date")date: String):String = {
    val total: Long = publisherService.getDauTotal(date)
    val totalAmount: Double = publisherService.getOrderTotalAmount(date)
    val result =
      s"""
         |[
         |    {"id":"dau","name":"新增日活","value":$total},
         |    {"id":"new_mid","name":"新增用户","value":233},
         |    {"id":"order_amount","name":"新增交易额","value":${totalAmount}}
         |]
         """.stripMargin
    result
  }

  @GetMapping(Array("realtime-hour"))
  def getRealTimeHour(@RequestParam("id") id: String, @RequestParam("date") date: String) = {
    if (id == "dau") {
      val todayMap: Map[String, Long] = publisherService.getDauHour(date)
      val yesterdayMap: Map[String, Long] = publisherService.getDauHour(date2Yesterday(date))

      val resultMap: mutable.Map[String, Map[String, Long]] = mutable.Map[String, Map[String, Long]]()
      resultMap += "today" -> todayMap
      resultMap += "yesterday" -> yesterdayMap
      println(resultMap)
      // 转变成 json 格式字符串输出
      import org.json4s.JsonDSL._
      JsonMethods.compact(JsonMethods.render(resultMap))
    } else if (id == "order_amount") {
      val todayMap: Map[String, Double] = publisherService.getOrderHourTotalAmount(date)
      val yesterdayMap: Map[String, Double] = publisherService.getOrderHourTotalAmount(date2Yesterday(date))
      val resultMap: mutable.Map[String, Map[String, Double]] = mutable.Map[String, Map[String, Double]]()
      resultMap += "today" -> todayMap
      resultMap += "yesterday" -> yesterdayMap
      println(resultMap)
      // 转变成 json 格式字符串输出
      import org.json4s.JsonDSL._
      JsonMethods.compact(JsonMethods.render(resultMap))
    } else {
      null
    }
  }

  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  // 通过今天计算出来昨天
  private def date2Yesterday(date: String): String = {
    val today: Date = dateFormat.parse(date)
    val yesterday: Date = DateUtils.addDays(today, -1)
    dateFormat.format(yesterday)
  }

  /*
  接口: http://localhost:8070/sale_detail?date=2019-05-20&&startpage=1&&size=5&&keyword=手机小米

   */
  @GetMapping(Array("sale_detail"))
  def saleDetail(@RequestParam("date") date: String,
                 @RequestParam("startpage") startPage: Int,
                 @RequestParam("size") pageSize: Int,
                 @RequestParam("keyword") keyword: String) = {
    val formater = new DecimalFormat(".00")

    // 按照性别统计
    val saleDetailMapByGender: Map[String, Any] = publisherService.getSaleDetailAndAggResultByAggField(date, keyword, startPage, pageSize, "user_gender", 2)
    val total: Integer = saleDetailMapByGender("total").asInstanceOf[Integer]
    val genderMap: Map[String, Long] = saleDetailMapByGender("aggMap").asInstanceOf[Map[String, Long]]
    val maleCount: Long = genderMap("M")
    val femalCount: Long = genderMap("F")
    // 个数变比例, F M 变 男 女 用户性别占比
    val genderOpts = List[Opt](
      Opt("total", total.toString),
      Opt("男", formater.format(maleCount.toDouble / total)),
      Opt("女", formater.format(femalCount.toDouble / total))
    )
    // 性别图片的数据
    val genderStat = Stat("用户性别占比", genderOpts)

    // 按照年龄统计
    val saleDetailMapByAge: Map[String, Any] = publisherService.getSaleDetailAndAggResultByAggField(date, keyword, startPage, pageSize, "user_age", 100)
    // 分别计算每个年龄段的人数
    val ageMap: Map[String, Long] = saleDetailMapByAge("aggMap").asInstanceOf[Map[String, Long]]
    val ageOpts = ageMap.groupBy {
      case (age, _) =>
        val intAge = age.toInt
        if (intAge < 20) "20岁以下"
        else if (intAge >= 20 && intAge <= 30) "20岁到30岁"
        else "30岁以上"
    }.map {
      case (age2age, map) =>
        (age2age, (0L /: map) (_ + _._2))
    }.map {
      case (age2age, count) =>
        (age2age, formater.format(count.toDouble / total))
    }.toList.map {
      case (age2age, rate) =>
        Opt(age2age, rate)
    }

    // 年龄段图表的数据
    val ageStat = Stat("用户年龄占比", ageOpts)

    // 详表
    val detailList: List[Map[String, Any]] = saleDetailMapByAge("detail").asInstanceOf[List[Map[String, Any]]]
    val saleInfo = SaleInfo(total, List(genderStat, ageStat), detailList)
    // 返回给前端数据
    import org.json4s.jackson.Serialization.write  // 序列化方法
    implicit val formats = DefaultFormats
    write(saleInfo)
  }


}
