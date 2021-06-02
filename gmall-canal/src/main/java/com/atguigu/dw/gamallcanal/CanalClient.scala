package com.atguigu.dw.gamallcanal

import java.net.InetSocketAddress
import java.util

import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, EventType, RowChange, RowData}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.google.protobuf.ByteString

/**
  * 这个试了很多次跑不了增量数据，用tempClient类模拟了
  */
object CanalClient {
  def main(args: Array[String]): Unit = {
    // 1. 创建能连接到 Canal 的连接器对象
    val connector: CanalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop104", 11111), "example", "", "")
    // 2. 连接到 Canal
    connector.connect()
    print("con="+connector.checkValid())
    // 3. 订阅数据，监控指定的表的数据的变化
    connector.subscribe("gmall.order_info")
    while (true){
      // 4. 获取消息  (一个消息对应 多条sql 语句的执行)
      val msg: Message = connector.get(100) // 一次最多获取 100 条 sql
      val entries: java.util.List[CanalEntry.Entry] = msg.getEntries
      import scala.collection.JavaConversions._
      //读数据，解析数据
//      if( entries.size()>0){
        // 6. 遍历每行数据
        for (entry<-entries){
          print("entry value="+entry.getStoreValue)
          // 7. EntryType.ROWDATA 只对这样的 EntryType 做处理
          if(entry.getEntryType == EntryType.ROWDATA){  //只处理row类型的
            // 8.获取到这行数据, 但是这种数据不是字符串, 所以要解析
            val value: ByteString = entry.getStoreValue
            print("entry2  value="+entry.getStoreValue)
            //把storevalue解析出来rowChange
            val rowChange: RowChange = RowChange.parseFrom(value)
            //一个storeValue中有多个RowData，每个RowData表示一个数据的变化
            val rowDatas: util.List[CanalEntry.RowData] = rowChange.getRowDatasList
            //解析rowDatas每行每列的数据
            print("tablenName="+entry.getHeader.getTableName+",eventType="+rowChange.getEventType)
            CanalHandler.handle(entry.getHeader.getTableName, rowChange.getEventType, rowChange.getRowDatasList)
          }
        }
//      } else {
//        println("没有抓取到数据...., 2s 之后重新抓取")
//        Thread.sleep(2000)
//      }
    }
  }
}
