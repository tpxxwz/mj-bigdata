package com.mj.sql.table._05tvf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class hello15Tumble {

    public static void main(String[] args) {
        // 运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 获取SourceTable 下面的INTERVAL '2' SECOND 表示 允许事件时间乱序 2 秒
        tableEnvironment.executeSql("""
                CREATE TABLE t_goods (
                  gid INT,
                  sales INT,
                  ts AS localtimestamp,
                  WATERMARK FOR ts AS ts - INTERVAL '2' SECOND
                ) WITH (
                   'connector' = 'datagen',
                   'rows-per-second' = '1',
                   'fields.gid.min' = '10',
                   'fields.gid.max' = '20',
                   'fields.sales.min' = '1',
                   'fields.sales.max' = '9'
                );
                """);
        // tableEnvironment.sqlQuery("select * from t_goods; ").execute().print();
        // 按照窗口进行查询--所有信息
        // tableEnvironment.sqlQuery("""
        //         SELECT * FROM TABLE(TUMBLE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS));
        //         """).execute().print();
        // 按照窗口进行查询--分组查询 下面的INTERVAL '2' SECOND 表示滚动窗口的长度，也就是每 5 秒聚合一次。
        tableEnvironment.sqlQuery("""
                SELECT window_start, window_end, gid, sum(sales) FROM TABLE
                (TUMBLE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS))
                GROUP BY window_start, window_end, gid;
                """).execute().print();
    }
}
