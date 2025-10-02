package com.mj.sql.table._06query;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Hello21TopN {

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
                   'rows-per-second' = '1',
                   'fields.gid.length' = '5',
                   'fields.type.min' = '1',
                   'fields.type.max' = '5',
                   'fields.price.min' = '100',
                   'fields.price.max' = '999'
                );
                """);
        // tableEnvironment.sqlQuery("select * from t_access").execute().print();
        // 开窗聚合计算 -- 时间范围
//        tableEnvironment.sqlQuery("""
//                select t.*, avg(price) OVER(
//                PARTITION BY type
//                ORDER BY ts
//                RANGE BETWEEN INTERVAL '10' SECONDS PRECEDING AND CURRENT ROW
//                ) as avg_price, NOW() pt from t_goods t
//                """).execute().print();
        // 开窗聚合计算 -- 计数范围
        tableEnvironment.sqlQuery("""
                select t.*, avg(price) OVER(
                    PARTITION BY type
                    ORDER BY ts
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) from t_goods t
                """).execute().print();
         // ROWS BETWEEN 2 PRECEDING   实际上是最新一条+前面两条算平均值
//        tableEnvironment.sqlQuery("""
//                select * from (
//                    select *, ROW_NUMBER() OVER (
//                        PARTITION BY type
//                        ORDER BY price desc
//                    ) AS rownum from t_goods
//                )where rownum <= 3
//                """).execute().print();


    }

}
