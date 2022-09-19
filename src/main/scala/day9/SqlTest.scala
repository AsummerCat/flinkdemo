package day9

import day2.Event
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * SQL语法编写查询Flink
 */
object SqlTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val eventStream = env.fromElements(
      Event("Alice", "./home", 1000L),
      Event("Bob", "./cart", 1000L),
      Event("Alice", "./prod?id=1", 5 * 1000L),
      Event("Cary", "./home", 60 * 1000L),
      Event("Bob", "./prod?id=3", 90 * 1000L),
      Event("Alice", "./prod?id=7", 105 * 1000L)
    )
    // 获取表环境
    val tableEnv = StreamTableEnvironment.create(env)
    // 将数据流转换成表
    val eventTable = tableEnv.fromDataStream(eventStream)
    // 用执行 SQL 的方式提取数据
    val visitTable = tableEnv.sqlQuery("select url, user from " + eventTable)
    // 将表转换成数据流，打印输出
    tableEnv.toDataStream(visitTable).print()
    // 执行程序
    env.execute()


    //    // 创建表环境
    //    val tableEnv = ...;
    //    // 创建输入表，连接外部系统读取数据
    //    tableEnv.executeSql("CREATE TEMPORARY TABLE inputTable ... WITH ( 'connector' = ... )")
    //    // 注册一个表，连接到外部系统，用于输出
    //    tableEnv.executeSql("CREATE TEMPORARY TABLE outputTable ... WITH ( 'connector' = ... )")
    //    // 执行 SQL 对表进行查询转换，得到一个新的表
    //    val table1 = tableEnv.sqlQuery("SELECT ... FROM inputTable... ")
    //    // 使用 Table API 对表进行查询转换，得到一个新的表
    //    val table2 = tableEnv.from("inputTable").select(...)
    //    // 将得到的结果写入输出表
    //    val tableResult = table1.executeInsert("outputTable")
  }


}
