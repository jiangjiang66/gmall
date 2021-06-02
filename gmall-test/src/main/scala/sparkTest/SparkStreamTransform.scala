package sparkTest

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamTransform {
  def main(args: Array[String]): Unit = {

    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //3.通过监控端口创建DStream，读进来的数据为一行行
    val stream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    val value: DStream[(String, Int)] = stream.transform(rdd => {
      rdd.flatMap(x => x.split(" ")).map((_, 1)).reduceByKey(_ + _)
    })
    value.print()
//    val flatMap: DStream[String] = stream.flatMap(x=>x.split(","))
//    val map: DStream[(String, Int)] = flatMap.map((_,1))
//
//    value.print()
//    val reduce: DStream[(String, Int)] = map.reduceByKey((t1, t2)=>t1+t2)
//    reduce.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
