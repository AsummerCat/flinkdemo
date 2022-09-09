package day4

import day2.Event
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._

import java.util.concurrent.TimeUnit

/**
 * flink文件流输出 文件addSink
 */
object SinkToFileTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val stream = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Alice", "./prod?id=100", 3000L),
      Event("Alice", "./prod?id=200", 3500L),
      Event("Bob", "./prod?id=2", 2500L),
      Event("Alice", "./prod?id=300", 3600L),
      Event("Bob", "./home", 3000L),
      Event("Bob", "./prod?id=1", 2300L),
      Event("Bob", "./prod?id=3", 3300L)
    )

    //文件路径 ,编码,字符集
    val fileSink = StreamingFileSink.forRowFormat(new Path("src/ioutput"), new SimpleStringEncoder[String]("UTF-8"))
      //通过.withRollingPolicy()方法指定“滚动策略”
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          //至少包含15分钟内的数据
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
          //最近5分钟没有收到新数据
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
          //当文件大小已达到1GB
          .withMaxPartSize(1024 * 1024 * 1024)
          .build()
      ).build

    stream.map(_.toString).addSink(fileSink)
    env.execute()
  }
}