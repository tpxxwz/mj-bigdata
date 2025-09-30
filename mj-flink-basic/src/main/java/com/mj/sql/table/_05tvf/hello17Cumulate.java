package com.mj.sql.table._05tvf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * CUMULATE(TABLE data, DESCRIPTOR(timecol), step, size)
 * data：拥有时间属性列的表。
 * timecol：列描述符，决定数据的哪个时间属性列应该映射到窗口。
 * step：指定连续的累积窗口之间增加的窗口大小。
 * size：指定累积窗口的最大宽度的窗口时间。size必须是step的整数倍。
 * offset：窗口的偏移量 [非必填]。
 * <p>
 * 月榜 周榜 隔一段时间要从0开始算 算也要一段一段时间算
 */
public class hello17Cumulate {

    public static void main(String[] args) {
        // 运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

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

        // 获取SourceTable 下面的INTERVAL '2' SECOND 表示 允许事件时间乱序 2 秒
        tableEnvironment.executeSql("""
                CREATE TABLE t_goods (
                  gid INT,
                  sales INT,
                  ts AS localtimestamp,
                  WATERMARK FOR ts AS ts - INTERVAL '2' SECOND
                ) WITH (
                   'connector' = 'datagen',
                   'rows-per-second' = '5',
                   'fields.gid.min' = '11',
                   'fields.gid.max' = '15',
                   'fields.sales.min' = '1',
                   'fields.sales.max' = '1'
                );
                """);
        tableEnvironment.sqlQuery("""
                SELECT window_start, window_end, gid, sum(sales) FROM TABLE
                (CUMULATE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS,≈))
                GROUP BY window_start, window_end, gid;
                """).execute().print();
    }
}
