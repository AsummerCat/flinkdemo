package day4

import day2.{ClickSource, Event}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * 生成数据addSink推送到redis
 */
object SinkToRedisTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").build()
    env.addSource(new ClickSource)
      //参数一:Jedis 的连接配置。
      //参数二:Redis 映射类接口，说明怎样将数据转换成可以写入 Redis 的类型
      .addSink(new RedisSink[Event](conf, new MyRedisMapper()))
    env.execute()
  }
}

class MyRedisMapper extends RedisMapper[Event] {
  override def getKeyFromData(t: Event): String = t.user

  override def getValueFromData(t: Event): String = t.url

  override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "clicks")
}