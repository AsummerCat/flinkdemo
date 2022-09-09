package day3

import day2.Event
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 转换算子的基本用法
 * FlatMap扁平化函数的使用
 */
object FlatMapTransTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L)
    )
    stream.flatMap(new MyFlatMap).print()
    env.execute()
  }

  //自定义FlatMap扁平化函数  FlatMapFunction
  class MyFlatMap extends FlatMapFunction[Event, String] {
    override def flatMap(value: Event, out: Collector[String]): Unit = {
      //如果是 Mary 的点击事件，则向下游发送 1 次，如果是 Bob 的点击事件，则向下游发送 2 次
      if (value.user.equals("Mary")) {
        out.collect(value.user)
      } else if (value.user.equals("Bob")) {
        out.collect(value.user)
        out.collect(value.user)
      }
    }
  }
}
