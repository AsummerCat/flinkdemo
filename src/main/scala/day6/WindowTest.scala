package day6

import day2.Event
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 滑动窗口
 * 1.窗口处理器
 * 2.窗口函数
 */
object WindowTest {
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

    //1.滚动窗口
    //一个长度为 5 秒的滚动窗口。
    stream.keyBy(_.user)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .aggregate()

    //1.滑动处理时间窗口
    //第一个参数:表示滑动窗口的大小，
    //第二个参数:表示滑动窗口的滑动步长。
    stream.keyBy(_.user)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .aggregate()

    //2.计数窗口
    //我们定义了一个长度为 10 的滚动计数窗口，当窗口中元素数量达到 10 的时候，就会触发 计算执行并关闭窗口。
    stream.keyBy(_.user)
      .countWindow(10)

    //2.计数窗口
    //我们定义了一个长度为 10、滑动步长为 3 的滑动计数窗口。每个窗口统计 10 个数据，每隔 3 个数据就统计输出一次结果
    stream.keyBy(_.user)
      .countWindow(10,3)

  }
}
