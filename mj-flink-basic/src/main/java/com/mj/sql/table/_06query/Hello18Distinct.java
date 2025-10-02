package com.mj.sql.table._06query;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Hello18Distinct {

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.executeSql("""
                CREATE TABLE t_access (
                  uid INT,
                  url STRING,
                  ts AS localtimestamp,
                  WATERMARK FOR ts AS ts - INTERVAL '2' SECOND
                ) WITH (
                   'connector' = 'datagen',
                   'rows-per-second' = '10',
                   'fields.uid.min' = '1000',
                   'fields.uid.max' = '2000',
                   'fields.url.length' = '10'
                );
                """);
//        tableEnvironment.sqlQuery("select * from t_access").execute().print();
        // 统计网站的UV和PV数
        tableEnvironment.sqlQuery("select count(url) as pv, count(distinct uid) as uv from t_access").execute().print();

    }

}
