package day5

import day2.Event
import org.apache.flink.streaming.api.scala._

/**
 * 生成水位线
 * 基础方法
 * assignTimestampsAndWatermarks()，它主要用来为流中的数据分配时间戳，并生成水位线来指示事件时间。
 */
object WaterMarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env
      .fromElements(
        Event("Mary", "./home", 1000L),
        Event("Bob", "./cart", 2000L)
      )

    //水位线生成策略
    //    val withTimestampsAndWatermarks = stream.assignTimestampsAndWatermarks()
    //水位线周期间隔
    env.getConfig.setAutoWatermarkInterval(60 * 1000L)

  }
}
