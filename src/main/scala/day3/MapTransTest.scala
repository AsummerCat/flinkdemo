package day3

import day2.Event
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

/**
 * 转换算子的基本用法
 * Map映射函数的使用
 */
object MapTransTest {
  def main(args: Array[String]): Unit = {

    //1.Map映射
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .fromElements(
        Event("Mary", "./home", 1000L),
        Event("Bob", "./cart", 2000L)
      )
    //1.使用匿名函数的方式提取 user 字段
    stream.map(_.user).print()
    //2.使用调用外部类的方式提取 user 字段
    stream.map(new UserExtractor).print()
    env.execute()
  }

  // 自定义映射函数 显式的实现 MapFunction 接口
  class UserExtractor extends MapFunction[Event, String] {
    override def map(value: Event): String = value.user
  }
}
