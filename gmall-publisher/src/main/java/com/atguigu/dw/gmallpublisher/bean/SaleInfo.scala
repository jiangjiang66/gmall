package com.atguigu.dw.gmallpublisher.bean

/**
  * 封装返回给前端的所有数据
  */
case class SaleInfo(total: Int, stats: List[Stat], detail: List[Map[String, Any]])
