package day2

import org.apache.flink.streaming.api.scala._

/**
 * 自定义Source数据源 ->读取数据
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


    env.execute("获取自定义数据源测试用例");
  }

}

//新增样例类
case class Event(user: String, url: String, timestamp: Long)