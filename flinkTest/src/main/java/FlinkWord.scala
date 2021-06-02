import org.apache.flink.api.scala._
object FlinkWord {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inputPath = "D:\\bigdataworkspace\\gmall\\in\\flink\\aa.txt"
    val text: DataSet[String] = env.readTextFile(inputPath)
    val group: GroupedDataSet[(String, Int)] = text.flatMap(x => x.split("  "))
      .map((_, 1))
      .groupBy(0)
    val result: AggregateDataSet[(String, Int)] = group.sum(1)
    result.print()
  }
}
