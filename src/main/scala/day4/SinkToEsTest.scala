package day4

import day2.Event
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util

/**
 * 生成数据addSink推送到es
 */
object SinkToEsTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
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

    //httpHosts：连接到的 Elasticsearch 集群主机列表。
    //elasticsearchSinkFunction：这并不是我们所说的 SinkFunction，而是用来说明具体处理逻辑、准备数据向 Elasticsearch 发送请求的函数。
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102", 9200, "http"))
    val esBuilder = new ElasticsearchSink.Builder[Event](
      httpHosts,
      new ElasticsearchSinkFunction[Event] {
        override def process(t: Event, runtimeContext: RuntimeContext,
                             requestIndexer: RequestIndexer): Unit = {
          val data = new java.util.HashMap[String, String]()
          data.put(t.user, t.url)
          val indexRequest = Requests
            .indexRequest()
            .index("clicks")
            .`type`("type")
            .source(data)
          requestIndexer.add(indexRequest)
        }
      }
    )
    stream.addSink(esBuilder.build())
    env.execute()
  }
}