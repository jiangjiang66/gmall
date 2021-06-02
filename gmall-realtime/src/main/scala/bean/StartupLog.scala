package bean
import java.text.SimpleDateFormat
import java.util.Date

import scala.beans.BeanProperty

case class StartupLog (
@BeanProperty     mid: String,
@BeanProperty     uid: String,
@BeanProperty     appId: String,
@BeanProperty     area: String,
@BeanProperty     os: String,
@BeanProperty     channel: String,
@BeanProperty     logType: String,
@BeanProperty     version: String,
@BeanProperty     ts: Long,
@BeanProperty   var logDate :String ="",
@BeanProperty         var logHour:String=""){
  val date = new Date(ts)
  logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
  logHour = new SimpleDateFormat("HH").format(date)
}