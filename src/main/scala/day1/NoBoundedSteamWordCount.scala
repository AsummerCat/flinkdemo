package day1

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * streaming无界数据流 flink基础语法
 */
object NoBoundedSteamWordCount {
  def main(args: Array[String]): Unit = {
    //1.创建一个流式执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //1.1 模拟从main启动类参数中获取参数 flink提供的API
    val hostName = ParameterTool.fromArgs(args).get("hostName")
    val port:Int = ParameterTool.fromArgs(args).get("port").toInt

    //2.模拟监听端口发送数据 表示获取实时的数据 ->无界数据流
    val lineDataStream = environment.socketTextStream(hostName, port)

    //3.对数据进行格式转换
    val wordAndOne = lineDataStream.flatMap(_.split(" ")).map(r => (r, 1))

    //4.对数据进行分组
    val wordAndOneUG = wordAndOne.keyBy(_._1)

    val value = wordAndOneUG.sum(1)
    // 5.打印输出
    value.print

    //6.执行任务 注意流数据处理需要执行调度任务
    environment.execute()
  }
}