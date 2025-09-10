package com.mj.sql.table.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Hello06ConnectorKafka {

    public static void main(String[] args) {
        // 执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 创建表：source
        tableEnvironment.executeSql("""
                CREATE TABLE flink_source_table (
                `deptno` INT,
                `dname` STRING,
                `loc` STRING
                ) WITH (
                'connector' = 'kafka',
                'topic' = 'kafka_source_topic',
                'properties.bootstrap.servers' = 'mj01:6667',
                'properties.group.id' = 'yjxxtliyi',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'csv'
                )
                """);

        // 创建表：SINK
        tableEnvironment.executeSql("""
                CREATE TABLE flink_sink_table (
                `deptno` INT,
                `dname` STRING,
                `loc` STRING
                ) WITH (
                'connector' = 'kafka',
                'topic' = 'kafka_sink_topic',
                'properties.bootstrap.servers' = 'mj01:6667',
                'properties.group.id' = 'yjxxtliyi',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
                )
                """);
        // 运行代码
        // tableEnvironment.sqlQuery("select * from flink_sink_table").execute().print();
        tableEnvironment.sqlQuery("select * from flink_source_table").insertInto("flink_sink_table").execute();
    }

}
