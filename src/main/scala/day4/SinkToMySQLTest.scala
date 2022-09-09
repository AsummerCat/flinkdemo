package day4

import day2.Event
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala._

import java.sql.PreparedStatement

/**
 * 生成数据addSink推送到mysql
 */
object SinkToMySQLTest {
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
    stream.addSink(
      JdbcSink.sink(
        "INSERT INTO clicks (user, url) VALUES (?, ?)",
        new JdbcStatementBuilder[Event] {
          override def accept(t: PreparedStatement, u: Event): Unit = {
            t.setString(1, u.user)
            t.setString(2, u.url)
          }
        },
        new
            JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl("jdbc:mysql://localhost:3306/test")
          .withDriverName("com.mysql.jdbc.Driver")
          .withUsername("username")
          .withPassword("password")
          .build()
      )
    )
    env.execute()
  }
}