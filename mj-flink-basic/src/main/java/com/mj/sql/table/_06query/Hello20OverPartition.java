package com.mj.sql.table._06query;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Hello20OverPartition {

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
                   'fields.type.max' = '1',
                   'fields.price.min' = '100',
                   'fields.price.max' = '999'
                );
                """);
        // 排序开窗函数--所有数据的排序
        tableEnvironment.sqlQuery("""
                select * from (
                    select *, ROW_NUMBER() OVER (
                        PARTITION BY type
                        ORDER BY price desc
                    ) AS rownum from t_goods
                )where rownum <= 3
                """).execute().print();


    }

}
