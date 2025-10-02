package com.mj.sql.table._06query;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Hello22TopNWindow {

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.executeSql("""
                CREATE TABLE t_goods (
                  gid STRING,
                  type INT,
                  price INT,
                  ts AS localtimestamp,
                  WATERMARK FOR ts AS ts - INTERVAL '2' SECOND
                ) WITH (
                   'connector' = 'datagen',
                   'rows-per-second' = '10',
                   'fields.gid.length' = '10',
                   'fields.type.min' = '1',
                   'fields.type.max' = '5',
                   'fields.price.min' = '100',
                   'fields.price.max' = '999'
                );
                """);
        // 窗口数据
//        tableEnvironment.sqlQuery("""
//                SELECT window_start, window_end, type, MAX(price) FROM TABLE
//                (TUMBLE (TABLE t_goods, DESCRIPTOR(ts), INTERVAL '10' SECONDS))
//                GROUP BY window_start, window_end,type
//                """).execute().print();
        // 查询每10秒内 销售额最高的前三种种类
        tableEnvironment.sqlQuery("""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY mp DESC) AS rownum FROM (
                        SELECT window_start, window_end, MAX(price) as mp, type FROM TABLE
                            (TUMBLE (TABLE t_goods, DESCRIPTOR(ts), INTERVAL '10' SECONDS))
                            GROUP BY window_start, window_end, type
                        )
                ) WHERE rownum <= 3
                """).execute().print();

        // 查询每10秒内 每个种类销售价格最高的前三名
        tableEnvironment.sqlQuery("""
                SELECT window_start, window_end, type,gid, price, rownum FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end, type ORDER BY price DESC) AS rownum FROM TABLE
                        (TUMBLE (TABLE t_goods, DESCRIPTOR(ts), INTERVAL '10' SECONDS))
                ) WHERE rownum <= 3
                """).execute().print();
    }

}
