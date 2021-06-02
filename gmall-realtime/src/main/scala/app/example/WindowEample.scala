package app.example

import org.apache.spark.sql.SparkSession
/*
spark-stream 的Window对象使用方法
 */
object WindowEample {

  def main(args: Array[String]): Unit = {
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

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val rankSpec = Window.partitionBy("seller_id")
      .orderBy(orders("price").desc)

    val shopOrderRank =
      orders.withColumn("rank", dense_rank.over(rankSpec))

    shopOrderRank.show()

    spark.close()


  }

}
