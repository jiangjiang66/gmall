package sparkTest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object sparkWord01 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setMaster("local").setAppName("WC")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    var word= List("aa","bb","aa","cc","aa","bb")
    val value = sc.makeRDD(word)
    val unit = value.map(x=>(x,1))
    val unit2: RDD[(String, Int)] = unit.reduceByKey((t1, t2)=>t1+t2)
    unit2.foreach(x=>print(x))
  }
}
