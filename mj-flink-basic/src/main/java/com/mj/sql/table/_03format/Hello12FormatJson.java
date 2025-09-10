package com.mj.sql.table._03format;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Hello12FormatJson {

    public static void main(String[] args) {
        // 运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 读取文件
        tableEnvironment.executeSql("""
                CREATE TABLE t_emp (
                   empno INT,
                   ename STRING,
                   job STRING,
                   mgr INT,
                   hiredate BIGINT,
                   sal DECIMAL(10, 2),
                   comm DECIMAL(10, 2),
                   deptno INT
                ) WITH (
                   'connector' = 'filesystem',
                   'path' = 'file:///Users/wjj/projects/mj-bigdata/mj-flink-basic/tmp/emp.txt',
                   'format' = 'json'
                )""");
        tableEnvironment.sqlQuery("select * from t_emp").execute().print();
    }

}
