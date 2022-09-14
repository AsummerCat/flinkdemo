package day6

import day2.Event
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 *
 * 窗口函数
 */
object WindowFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .fromElements(
        Event("Mary", "./home", 1000L),
        Event("Bob", "./cart", 2000L)
      ).assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps[Event]()
        .withTimestampAssigner(
          new SerializableTimestampAssigner[Event] {
            override def extractTimestamp(element: Event, recordTimestamp: Long):
            Long = element.timestamp
          }
        )
    )

    //    stream.map(data => (data.user, 1))
    //      .keyBy(_._1)
    //      //设置一个基于时间时间的滚动窗口 长度5秒的滚动窗口
    //      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    //      //1.归约函数(ReduceFunction)
    //      //计数聚合
    //      .reduce((state, data) => (data._1, state._2 + data._2))
    //      .print()

    stream.keyBy(data => true)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .aggregate(new AvgPv)
      .print()


  }
}

//自定义窗口函数
class AvgPv extends AggregateFunction[Event, (Set[String], Double), Double] {
  // 创建空累加器，类型是元组，元组的第一个元素类型为 Set 数据结构，用来对用户名进行去重
  // 第二个元素用来累加 pv 操作，也就是每来一条数据就加一
  override def createAccumulator(): (Set[String], Double) = (Set[String](), 0L)

  // 累加规则
  override def add(value: Event, accumulator: (Set[String], Double)):
  (Set[String], Double) = (accumulator._1 + value.user, accumulator._2 + 1L)

  // 获取窗口关闭时向下游发送的结果
  override def getResult(accumulator: (Set[String], Double)): Double =
    accumulator._2 / accumulator._1.size

  // merge 方法只有在事件时间的会话窗口时，才需要实现，这里无需实现。
  override def merge(a: (Set[String], Double), b: (Set[String], Double)):
  (Set[String], Double) = ???
}
