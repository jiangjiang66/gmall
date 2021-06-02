package bean

import java.text.SimpleDateFormat
import java.util.Date

/**
  * 事件日志样例类
  * @param mid
  * @param uid
  * @param appId
  * @param area
  * @param os
  * @param logType
  * @param eventId
  * @param pageId
  * @param nextPageId
  * @param itemId
  * @param ts
  * @param logDate
  * @param logHour
  */
case class EventLog(mid: String,
                    uid: String,
                    appId: String,
                    area: String,
                    os: String,
                    logType: String,
                    eventId: String,
                    pageId: String,
                    nextPageId: String,
                    itemId: String,
                    ts: Long,
                    var logDate :String ="",
                    var logHour:String=""){
    val date = new Date(ts)
    logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
    logHour = new SimpleDateFormat("HH").format(date)
}