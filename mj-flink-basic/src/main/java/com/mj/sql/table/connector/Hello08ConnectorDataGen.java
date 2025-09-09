package com.mj.sql.table.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class Hello08ConnectorDataGen {

    public static void main(String[] args) {
        // 运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 获取SourceTable
        tableEnvironment.executeSql("""
                CREATE TABLE t_datagen (
                  f_sequence INT,
                  f_random INT,
                  f_random_str STRING,
                  ts AS localtimestamp,
                  WATERMARK FOR ts AS ts
                ) WITH (
                   'connector' = 'datagen',
                   'rows-per-second' = '5',
                   'fields.f_sequence.kind' = 'sequence',
                   'fields.f_sequence.start' = '1',
                   'fields.f_sequence.end' = '1000',
                   'fields.f_random.min' = '1',
                   'fields.f_random.max' = '1000',
                   'fields.f_random_str.length' = '10'
                );
                """);
        // 执行SQL
        tableEnvironment.sqlQuery("select * from t_datagen").execute().print();

    }

}
