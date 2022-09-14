package day5

import day2.Event
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._

import java.time.Duration


/**
 * Flink内置水位线策略
 * 1. 有序流
 * 2. 乱序流
 */
object WaterMarkInnerTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env
      .fromElements(
        Event("Mary", "./home", 1000L),
        Event("Bob", "./cart", 2000L)
      )



    //水位线周期间隔
    env.getConfig.setAutoWatermarkInterval(60 * 1000L)

    //1.有序流  水位线生成策略 根据字段中的timestamp来排序
    stream.assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps[Event]()
        .withTimestampAssigner(
          new SerializableTimestampAssigner[Event] {
            override def extractTimestamp(element: Event, recordTimestamp: Long):
            Long = element.timestamp
          }
        )
    )



    //2.乱序流 插入水位线的逻辑
    stream.assignTimestampsAndWatermarks(
      //针对乱序流插入水位线，延迟时间设置为 5s
      WatermarkStrategy
        .forBoundedOutOfOrderness[Event](Duration.ofSeconds(5))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[Event] {
            // 指定数据中的哪一个字段是时间戳
            override def extractTimestamp(element: Event, recordTimestamp: Long):
            Long = element.timestamp
          }
        )
    )
  }
}
