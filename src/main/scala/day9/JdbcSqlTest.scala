package day8

/**
 * SQL语法连接外部的mysql数据库
 */
object JdbcSqlTest {
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

    //这里在 WITH 前使用了 PARTITIONED BY 对数据进行了分区操作。文件系统连接器支持
    //对分区文件的访问。
  tableEnv.executeSql("""
CREATE TABLE MyTable (
 id BIGINT,
 name STRING,
 age INT,
 status BOOLEAN,
 PRIMARY KEY (id) NOT ENFORCED
) WITH (
 'connector' = 'jdbc',
 'url' = 'jdbc:mysql://localhost:3306/mydatabase',
 'table-name' = 'users'
);
    """)

    // 将表转换成数据流，打印输出
    tableEnv.toDataStream(visitTable).print()
    // 执行程序
    env.execute()
  }


}
