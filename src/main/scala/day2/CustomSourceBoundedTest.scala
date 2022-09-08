package day2

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

import java.util.Calendar
import scala.util.Random

/**
 * 自定义Source数据源 ->读取数据
 */
object CustomSourceBoundedTest {
  def main(args: Array[String]): Unit = {
    //1.创建一个流式执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行任务的数量为1
    env.setParallelism(1)

    //2.从自定义数据源中读取数据
    val stream: DataStream[Event] = env.addSource(new ClickSource)
    stream.print()

    env.execute("获取自定义数据源测试用例");
  }

}

//新增样例类
case class Event(user: String, url: String, timestamp: Long)

// 实现 SourceFunction 接口，接口中的泛型是自定义数据源中的类型
class ClickSource extends SourceFunction[Event] {
  // 标志位，用来控制循环的退出
  var running = true

  //重写 run 方法，使用上下文对象 sourceContext 调用 collect 方法
  override def run(ctx: SourceContext[Event]): Unit = {
    // 实例化一个随机数发生器
    val random = new Random
    // 供随机选择的用户名的数组
    val users = Array("Mary", "Bob", "Alice", "Cary")
    // 供随机选择的 url 的数组
    val urls = Array("./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2")
    //通过 while 循环发送数据，running 默认为 true，所以会一直发送数据
    while (running) {
      // 调用 collect 方法向下游发送数据
      ctx.collect(
        Event(
          users(random.nextInt(users.length)), // 随机选择一个用户名
          urls(random.nextInt(urls.length)), // 随机选择一个 url
          Calendar.getInstance.getTimeInMillis // 当前时间戳
        )
      )
      // 隔 1 秒生成一个点击事件，方便观测
      Thread.sleep(1000)
    }
  }

  //通过将 running 置为 false 终止数据发送循环
  override def cancel(): Unit = running = false
}