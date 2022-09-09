package day3

import day2.Event
import org.apache.flink.streaming.api.scala._

/**
 * 转换算子的基本用法
 * 基础函数 sum ,min,max 等
 */
object BaseTransTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.fromElements(("a", 1), ("a", 3), ("b", 3), ("b", 4))
    stream.keyBy(_._1).sum(1).print() //对元组的索引 1 位置数据求和
    stream.keyBy(_._1).sum("_2").print() //对元组的第 2 个位置数据求和
    stream.keyBy(_._1).max(1).print() //对元组的索引 1 位置求最大值
    stream.keyBy(_._1).max("_2").print() //对元组的第 2 个位置数据求最大值
    stream.keyBy(_._1).min(1).print() //对元组的索引 1 位置求最小值
    stream.keyBy(_._1).min("_2").print() //对元组的第 2 个位置数据求最小值
    stream.keyBy(_._1).maxBy(1).print() //对元组的索引 1 位置求最大值
    stream.keyBy(_._1).maxBy("_2").print() //对元组的第 2 个位置数据求最大值
    stream.keyBy(_._1).minBy(1).print() //对元组的索引 1 位置求最小值
    stream.keyBy(_._1).minBy("_2").print() //对元组的第 2 个位置数据求最小值
    env.execute()
  }

}

object TransAggregationCaseClass {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.fromElements(
      Event("Mary", "./home", 2000L),
      Event("Mary", "./home", 4000L),
      Event("Bob", "./cart", 2000L),
      Event("Bob", "./cart", 4000L)
    ) // 使用 user 作为分组的字段，并计算最大的时间戳
    stream.keyBy(_.user).maxBy("timestamp").print()
    env.execute()
  }
}