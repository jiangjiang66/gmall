package sparkTest

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamTest {
  def main(args: Array[String]): Unit = {
    // 定义更新状态方法，参数values为当前批次单词频度，state为以往批次单词频度
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("in")
    //3.通过监控端口创建DStream，读进来的数据为一行行
    val stream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    val flatMap: DStream[String] = stream.flatMap(x=>x.split(","))
    val map: DStream[(String, Int)] = flatMap.map((_,1))
    val value: DStream[(String, Int)] = map.updateStateByKey[Int](updateFunc)
    value.print()
//    val reduce: DStream[(String, Int)] = map.reduceByKey((t1, t2)=>t1+t2)
//    reduce.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
