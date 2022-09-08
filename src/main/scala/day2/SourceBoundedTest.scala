package day2

import org.apache.flink.streaming.api.scala._

/**
 * source 多方式读取数据源 ->读取数据
 */
object SourceBoundedTest {
  def main(args: Array[String]): Unit = {
    //1.创建一个流式执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.从指定元素中读取数据
    val value = env.fromElements(1, 2, 3, 4)
    value.print()

    env.execute("算子测试用例");
  }
}
