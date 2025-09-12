package com.mj.sql.table._04watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Hello13ProcessTime {

    public static void main(String[] args) {
        // 执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 创建表：Source [10,ACCOUNTIING,NEWYORK]
        tableEnvironment.executeSql("""
                CREATE TABLE t_dept_sql (
                   `deptno` INT,
                   `dname` STRING,
                   `loc` STRING,
                   `pt` AS PROCTIME()
                ) WITH (
                   'connector' = 'kafka',
                   'topic' = 'kafka_topic_dept',
                   'properties.bootstrap.servers' = 'mj01:6667',
                   'properties.group.id' = 'sql',
                   'scan.startup.mode' = 'earliest-offset',
                   'format' = 'csv'
                )""");
//        tableEnvironment.sqlQuery("select * from t_dept_sql").execute().print();
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("mj-flink-basic/tmp/dept.txt"))
                .build();
        DataStreamSource<String> deptSource = environment.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "file-source");
        Table table1 = tableEnvironment.fromDataStream(
                deptSource,
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .columnByExpression("pt", "PROCTIME()")
                        .build());
        tableEnvironment.sqlQuery("select * from " + table1.toString()).execute().print();
    }

}
