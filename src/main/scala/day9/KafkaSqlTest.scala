package day8

/**
 * SQL语法编写查询Flink
 */
object KafkaSqlTest {
  def main(args: Array[String]): Unit = {
    val eventStream = env
      .fromElements(
        Event("Alice", "./home", 1000L),
        Event("Bob", "./cart", 1000L),
        Event("Alice", "./prod?id=1", 5 * 1000L),
        Event("Cary", "./home", 60 * 1000L),
        Event("Bob", "./prod?id=3", 90 * 1000L),
        Event("Alice", "./prod?id=7", 105 * 1000L)
      )
    // 获取表环境
    val eventStream = StreamTableEnvironment.create(env)
    // 将数据流转换成表
    val eventTable = tableEnv.fromDataStream(eventStream)

  tableEnv.executeSql("""
    CREATE TABLE KafkaTable (
      `user` STRING,
      `url` STRING,
      `ts` TIMESTAMP(3) METADATA FROM 'timestamp'
    ) WITH (
      'connector' = 'kafka',
    'topic' = 'events',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'testGroup',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'csv'
    )
    """)

    // 将表转换成数据流，打印输出
    tableEnv.toDataStream(visitTable).print()
    // 执行程序
    env.execute()
  }


}
