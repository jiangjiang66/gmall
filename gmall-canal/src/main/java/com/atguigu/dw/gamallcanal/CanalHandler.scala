package com.atguigu.dw.gamallcanal

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{EventType, RowData}
import com.atguigu.dw.gamallcanal.kafkaSender.MyKafkaSender
import com.wuhui.common.Constant
import org.spark_project.guava.base.CaseFormat
import java.util

object CanalHandler {
  /**
    * 处理从 canal 取来的数据
    *
    * @param tableName   表名
    * @param eventType   事件类型
    * @param rowDataList 数据类别
    */
  def handle(tableName: String, rowDataList: util.List[CanalEntry.RowData], eventType: CanalEntry.EventType) = {
    if ("order_info" == tableName && eventType == EventType.INSERT && rowDataList != null && !rowDataList.isEmpty) {
      sendRowListToKafka(rowDataList, Constant.TOPIC_ORDER)
    } else if ("order_detail" == tableName && eventType == EventType.INSERT && rowDataList != null && !rowDataList.isEmpty) {
      sendRowListToKafka(rowDataList, Constant.TOPIC_ORDER_DETAIL)
    }
  }

  private def sendRowListToKafka(rowDataList: util.List[CanalEntry.RowData], topic: String): Unit = {
    for (rowData <- rowDataList) {
      val jsonObj = new JSONObject()
      // 变化后的列
      val columnList: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
      for (column <- columnList) {
        jsonObj.put(column.getName, column.getValue)
      }
      println(jsonObj.toJSONString)
      // 写入到Kafka
      MyKafkaSender.sendToKafka(topic, jsonObj.toJSONString)
    }
  }

}
