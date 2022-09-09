package day3

import day2.Event
import org.apache.flink.streaming.api.scala._

/**
 * 转换算子的基本用法
 * KeyBy函数的使用 ,根据key进行分组
 */
object KeyByTransTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val stream = env
      .fromElements(
        Event("Mary", "./home", 1000L),
        Event("Bob", "./cart", 2000L),
        Event("Aline", "./aline", 2000L)
      ) //指定 Event 的 user 属性作为 key
    val keyedStream = stream.keyBy(_.user)
    keyedStream.print()
    env.execute()
  }
}
