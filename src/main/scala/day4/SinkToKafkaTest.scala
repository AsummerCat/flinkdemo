package day4

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

import java.util.Properties

/**
 * 生成数据addSink推送到kafka
 */
object SinkToKafkaTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val properties = new Properties()
    properties.put("bootstrap.servers", "hadoop102:9092")
    val stream = env.readTextFile("input/clicks.csv")
    stream.addSink(new FlinkKafkaProducer[String]("clicks", new SimpleStringSchema(), properties))
    env.execute()
  }
}