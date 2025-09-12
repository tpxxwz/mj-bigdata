package com.mj.sql.table._04watermark;

import com.mj.bean.WaterMarkData;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Random;

public class Hello14EventTime {

    public static void main(String[] args) {
        // 执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 创建表：SQL
        tableEnvironment.executeSql("""
                CREATE TABLE t_dept_sql (
                   `deptno` INT,
                   `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
                   `dname` STRING,
                   `loc` STRING,
                   WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
                ) WITH (
                   'connector' = 'kafka',
                   'topic' = 'kafka_topic_dept',
                   'properties.bootstrap.servers' = 'mj01:6667',
                   'properties.group.id' = 'sql',
                   'scan.startup.mode' = 'earliest-offset',
                   'format' = 'csv'
                )""");
//        tableEnvironment.sqlQuery("select * from t_dept_sql").execute().print();
        // 创建表: SQL
        Random random = new Random();
        long start = System.currentTimeMillis();
        DataGeneratorSource<WaterMarkData> dataGeneratorSource = new DataGeneratorSource<>(
                counter -> {
                    String userId = String.valueOf(random.nextInt(1000) + 1);
                    Long eventTime = start + counter;
                    return new WaterMarkData(userId, eventTime, 0);
                },
                2,
                RateLimiterStrategy.perSecond(10),
                Types.POJO(WaterMarkData.class));
        DataStream<WaterMarkData> stream1 = environment.fromSource(
                dataGeneratorSource,
                WatermarkStrategy.noWatermarks(),     // 不使用水印策略
                "datagenerator"                       // 数据源名称
        );
        // 4. 打印数据流
        stream1.print();
        WatermarkStrategy<WaterMarkData> watermarkStrategy = WatermarkStrategy.<WaterMarkData>forBoundedOutOfOrderness(
                        Duration.ofSeconds(5))
                .withTimestampAssigner((wm, l) -> wm.getEventTime());
        DataStream<WaterMarkData> stream = environment.fromSource(
                dataGeneratorSource,
                watermarkStrategy,
                "datagenerator");

        Table table = tableEnvironment.fromDataStream(
                stream,
                Schema.newBuilder()
                        .column("userId", DataTypes.STRING())
                        .column("eventTime", DataTypes.BIGINT())
                        .columnByExpression("pt", "NOW()")
                        .columnByExpression("et", "TO_TIMESTAMP(FROM_UNIXTIME(eventTime/1000))")
                        .watermark("et", "et - INTERVAL '5' SECOND")
                        .build());
        tableEnvironment.sqlQuery("select * from " + table.toString()).execute().print();
    }

}
