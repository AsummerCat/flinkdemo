package day8

import day2.Event
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 懒加载 值状态
 */
object LazyValueStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env
      .fromElements(
        Event("Mary", "./home", 1000L),
        Event("Bob", "./cart", 2000L)
      )


    val result = stream
      .keyBy(_.url)
      .flatMap(new MyMap)
      .print()

    env.execute()
  }

  /**
   * 自定义 RichFlatMapFunction
   */

  class MyMap extends RichFlatMapFunction[Event, String] {
    //定义一个值状态
    lazy val valueState: ValueState[Event] = getRuntimeContext.getState(new ValueStateDescriptor[Event]("my-value", classOf[Event]))

    override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

    //开启函数初始化配置
    override def open(parameters: Configuration): Unit = Unit

    //具体自定义函数
    override def flatMap(in: Event, collector: Collector[String]): Unit = {
      //对状态进行操作
      println("值状态为" + valueState.value())
      //更新值状态的值
      valueState.update(in)
    }
  }
}
