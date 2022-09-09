package day3

import day2.Event
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
 * 转换算子的基本用法
 * Filter过滤函数的使用
 */
object FilterTransTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .fromElements(
        Event("Mary", "./home", 1000L),
        Event("Bob", "./cart", 2000L)
      )
    //过滤出用户名是 Mary 的数据
    stream.filter(_.user.equals("Mary")).print()
    stream.filter(new UserFilter).print()
    env.execute()
  }

  //自定义过滤函数
  class UserFilter extends FilterFunction[Event] {
    override def filter(value: Event): Boolean = value.user.equals("Mary")
  }
}
