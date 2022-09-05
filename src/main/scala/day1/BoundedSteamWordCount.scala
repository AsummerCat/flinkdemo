package day1

import org.apache.flink.streaming.api.scala._

/**
 * streaming有界数据流 flink基础语法
 */
object BoundedSteamWordCount {
  def main(args: Array[String]): Unit = {
    //1.创建一个流式执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.读取文本文件数据 获取DataStream
    val lineDataStream = environment.readTextFile("input/words.txt")

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