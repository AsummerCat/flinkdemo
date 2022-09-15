package day8

import day2.Event
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector


/**
 * Flink针对状态生存时间的设置
 * TTL
 */
object TtlTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env
      .fromElements(
        Event("Mary", "./home", 1000L),
        Event("Bob", "./cart", 2000L)
      )


    val result = stream
      .keyBy(_.url)
      .flatMap(new MyMap)
      .print()

    env.execute()
  }

  /**
   * 自定义 RichFlatMapFunction
   */

  class MyMap extends RichFlatMapFunction[Event, String] {
    //TTL设置
    //1.过期时间为当前时间+TTL时间
    val ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
      //2.设置更新类型
      //OnCreateAndWrite 表示只有创建状态和更改状态（写操作）时更新失效时间
      //OnReadAndWrite 则表示无论读写操作都会更新失效时间，也就是只要对状态进行了访问
      //默认OnCreateAndWrite
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      //3.设置状态的可见性。所谓的“状态可见性”
      //NeverReturnExpired  失效就返回空
      //ReturnExpiredIfNotCleanedUp 失效后未被及时clear,还会返回数据
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build()

    //定义一个映射状态

    lazy val mapState: MapState[Long, Event] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Event]("window-pv", classOf[String], classOf[Event]).enableTimeToLive(ttlConfig))

    //失效时间 这部分内容需要加入到 new MapStateDescriptor知乎
    //    mapState.enableTimeToLive(ttlConfig)


    override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

    //开启函数初始化配置
    override def open(parameters: Configuration): Unit = {

    }

    //具体自定义函数
    override def flatMap(in: Event, collector: Collector[String]): Unit = {
      //对状态进行操作
      mapState.put(in.timestamp, in)

    }
  }
}
