package day7

import day2.Event
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration


/**
 * Flink迟到数据处理
 */
object LastDataWaterTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env
      .fromElements(
        Event("Mary", "./home", 1000L),
        Event("Bob", "./cart", 2000L)
      )



    //水位线周期间隔
    env.getConfig.setAutoWatermarkInterval(60 * 1000L)

    //水位线设置
    stream.assignTimestampsAndWatermarks(
      WatermarkStrategy
        //方式一.迟到数据处理 最大延迟时间设置为 5 秒钟
        .forBoundedOutOfOrderness[Event](Duration.ofSeconds(5))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[Event] {
            override def extractTimestamp(element: Event, recordTimestamp: Long):
            Long = element.timestamp
          }
        )
    )

    // 定义侧输出流标签
    val outputTag = OutputTag[Event]("late")

    val result = stream
      .keyBy(_.url)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      // 方式二：允许窗口处理迟到数据，设置 1 分钟的等待时间
      .allowedLateness(Time.minutes(1))
      // 方式三：将最后的迟到数据输出到侧输出流
      .sideOutputLateData(outputTag)


    env.execute()


  }
}
