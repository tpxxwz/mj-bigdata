package com.mj.sql.table._05tvf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class hello16Hop {

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
                   'fields.gid.kind' = 'sequence',
                   'fields.gid.start' = '1',
                   'fields.gid.end' = '1000',
                   'fields.sales.min' = '1',
                   'fields.sales.max' = '1'
                );
                """);
        // tableEnvironment.sqlQuery("select * from t_goods; ").execute().print();
        // 查询表信息
        // tableEnvironment.sqlQuery("""
        //         SELECT * FROM TABLE(HOP(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS, INTERVAL '10' SECONDS));
        //         """).execute().print();
        // 查询表信息
//        tableEnvironment.sqlQuery("""
//                SELECT window_start, window_end, sum(sales) FROM TABLE
//                (HOP(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS, INTERVAL '10' SECONDS))
//                GROUP BY window_start, window_end;
//                """).execute().print();
        tableEnvironment.sqlQuery("""
                SELECT window_start, window_end, gid, sum(sales) FROM TABLE
                (HOP(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS, INTERVAL '10' SECONDS))
                GROUP BY window_start, window_end, gid;
                """).execute().print();
    }
}
