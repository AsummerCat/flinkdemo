package day8

import day2.Event
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
 * Flink的映射状态 (MapState)
 */
object MapStateTest {
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
    //定义一个映射状态
    var mapState: MapState[Long, Event] = _


    override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

    //开启函数初始化配置
    override def open(parameters: Configuration): Unit = {
      mapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Event]("window-pv", classOf[String], classOf[Event]))
    }

    //具体自定义函数
    override def flatMap(in: Event, collector: Collector[String]): Unit = {
      //对状态进行操作
      mapState.put(in.timestamp, in)
    }
  }
}
