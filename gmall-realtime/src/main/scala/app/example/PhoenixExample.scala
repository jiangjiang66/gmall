package app.example

import org.apache.spark.sql.SparkSession

/*
将数据通过phoenix将数据写到hbase
 */
class PhoenixExample {
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("WindowTest")
    .getOrCreate()
  import spark.implicits._

  val orders = Seq(
    ("1", "s1", "2017-05-01", 100),
    ("2", "s1", "2017-05-02", 200),
    ("3", "s1", "2017-05-02", 200),
    ("4", "s2", "2017-05-01", 300),
    ("5", "s2", "2017-05-01", 100),
    ("6", "s3", "2017-05-01", 100),
    ("6", "s3", "2017-05-02", 50)
  ).toDF("order_id", "seller_id", "order_date", "price")
  // 参数1: 表名  参数2: 列名组成的 seq 参数 zkUrl: zookeeper 地址
  import org.apache.phoenix.spark._

//  orders.saveToPhoenix(
//    "tmp_order",
//    Seq("order_id", "seller_id", "order_date", "price"),
//    Some("hadoop201,hadoop202,hadoop203:2181")
//  )

}
