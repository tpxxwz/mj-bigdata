package com.mj.sql.table.schema;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Hello11Schema {

    public static void main(String[] args) {
        // 执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 创建表：Source [10,ACCOUNTIING,NEWYORK]
        tableEnvironment.executeSql("""
                CREATE TABLE t_dept_sql (
                   `deptno` INT,
                   `deptno_new` AS deptno * 100,
                   `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
                   `dname` STRING,
                   `loc` STRING
                ) WITH (
                   'connector' = 'kafka',
                   'topic' = 'kafka_topic_dept',
                   'properties.bootstrap.servers' = 'mj01:6667',
                   'properties.group.id' = 'yjxxtliyi',
                   'scan.startup.mode' = 'earliest-offset',
                   'format' = 'csv'
                )""");
        tableEnvironment.createTable("t_dept_tableapi", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("deptno", DataTypes.INT())
                        .columnByExpression("deptno_new", "deptno * 100")
                        .columnByMetadata("event_time", DataTypes.TIMESTAMP_LTZ(3), "timestamp", true)
                        .column("dname", DataTypes.STRING())
                        .column("loc", DataTypes.STRING())
                        .build())
                .option("connector", "kafka")
                .option("topic", "kafka_topic_dept")
                .option("properties.bootstrap.servers", "mj01:6667")
                .option("properties.group.id", "tableapi")
                .option("scan.startup.mode", "earliest-offset")
                .format("csv")
                .build());
        tableEnvironment.sqlQuery("select * from t_dept_tableapi").execute().print();
    }

}
