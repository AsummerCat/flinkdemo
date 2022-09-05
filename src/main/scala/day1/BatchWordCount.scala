package day1

object BatchWordCount {
  def main(args: Array[String]): Unit = {
    //1.创建执行环境并配置并行度
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2.读取文本文件 ->DataSet格式
    val lineDS = env.readTextFile("input/words.txt")
    //3.对数据进行格式转换
    val wordAndOne = lineDS.flatMap(_.split(" ")).map(r => (r, 1))
    //4.对数据进行分组
    val wordAndOneUG = wordAndOne.groupByKey(_ + _)
    // 5.打印输出
    wordAndOneUG.print
  }
}