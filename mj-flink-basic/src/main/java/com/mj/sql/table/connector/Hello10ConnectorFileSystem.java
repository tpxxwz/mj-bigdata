package com.mj.sql.table.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Hello10ConnectorFileSystem {

    public static void main(String[] args) {
        // 运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 读取文件
        tableEnvironment.executeSql("""
                CREATE TABLE t_dept (
                   deptno INT,
                   dname STRING,
                   loc STRING
                ) WITH (
                   'connector' = 'filesystem',
                   'path' = 'file:///USers/wjj/projects/mj-bigdata/mj-flink-basic/tmp/dept.txt',
                   'format' = 'csv'
                )""");
        tableEnvironment.sqlQuery("select * from t_dept").execute().print();
    }

}
