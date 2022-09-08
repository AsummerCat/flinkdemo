package day2

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * source 多方式读取数据源 ->读取数据
 */
object SourceBoundedTest {
  def main(args: Array[String]): Unit = {
    //1.创建一个流式执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行任务的数量为1
    env.setParallelism(1)

    //2.从指定元素中读取数据
    val value: DataStream[Int] = env.fromElements(1, 2, 3, 4)
    value.print()

    //3.从指定列表读取数据
    val value1 = env.fromCollection(List(Event("Mary", "/.home", 1000L), Event("Bob", "/.cart", 2000L)))
    //3.1 我们也可以不构建集合，直接将元素列举出来，调用 fromElements 方法进行读取数据：
    val value2 = env.fromElements(Event("Mary", "/.home", 1000L), Event("Bob", "/.cart", 2000L))
    value1.print()

    //4.从文件读取数据 可以是目录,也可以是文件
    val stream = env.readTextFile("clicks.csv")

    //5.从 Socket 读取数据
    val stream1 = env.socketTextStream("localhost", 7777)

    //6.从 Kafka 读取数据 需要增加依赖 flink-connector-kafka_${scala.binary.version}
    //目前最新版本 只支持 0.10.0 版本以上的 Kafka
    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    //创建一个 FlinkKafkaConsumer 对象，传入必要参数，从 Kafka 中读取数据
    //第一个参数: 主题TOP
    //第二个参数: 反序列化方式  使用的 SimpleStringSchema，是一个内置的 DeserializationSchema
    //第三个参数: 第三个参数是一个 Properties 对象，设置了 Kafka 客户端的一些属性
    val kafkaStream = env.addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties))
    kafkaStream.print("kafka")


    env.execute("算子测试用例");
  }

}

//新增样例类
case class Event(user: String, url: String, timestamp: Long)