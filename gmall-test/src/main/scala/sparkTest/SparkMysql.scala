package sparkTest

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkMysql {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/gmall"
    val userName = "root"
    val passWd = "jiang"

    val rdd: JdbcRDD[(Int, String)] = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from testhehe where id >= ? and id >=?"
      , 1,
      1,
      1,
      r => (r.getInt(1), r.getString(2))
    )
    //打印最后结果
    println(rdd.count())
    rdd.foreach(println)

    sc.stop()
  }
}
