package com.mj.sql.table.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class Hello09ConnectorKafkaUpsert {

    public static void main(String[] args) throws InterruptedException {
        // 运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 获取SourceTable
        tableEnvironment.executeSql("""
                CREATE TABLE t_dept (
                  deptno INT,
                  salenum INT,
                  ts AS localtimestamp,
                  WATERMARK FOR ts AS ts
                ) WITH (
                   'connector' = 'datagen',
                   'rows-per-second' = '1',
                   'fields.deptno.min' = '88',
                   'fields.deptno.max' = '99',
                   'fields.salenum.min' = '1',
                   'fields.salenum.max' = '9'
                )""");
        TableResult tableResult1 = tableEnvironment.from("t_dept").execute();
        new Thread(tableResult1::print).start();
        // 插入到一张Kafka的表中
        tableEnvironment.executeSql("""
                CREATE TABLE flink_dept_sale_sum (
                  deptno INT,
                  sumsale INT,
                  PRIMARY KEY (deptno) NOT ENFORCED
                ) WITH (
                  'connector' = 'upsert-kafka',
                  'topic' = 'topic_dept_sale_sum',
                  'properties.bootstrap.servers' = 'mj01:6667',
                  'key.format' = 'csv',
                  'value.format' = 'json'
                );
                """);
        // tableEnvironment.sqlQuery("select * from flink_dept_sale_sum_for_check").execute().print();
        TableResult tableResult2 = tableEnvironment.from("flink_dept_sale_sum").execute();
        new Thread(tableResult2::print).start();
        // 插入数据
        tableEnvironment.executeSql("insert into flink_dept_sale_sum select deptno, sum(salenum) as sumsale from t_dept group by deptno");

    }

}
