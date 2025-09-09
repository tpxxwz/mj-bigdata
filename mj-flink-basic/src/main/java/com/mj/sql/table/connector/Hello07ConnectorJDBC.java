package com.mj.sql.table.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class Hello07ConnectorJDBC {

    public static void main(String[] args) {
        String url = System.getenv("ALIYUN_MYSQL_DB_URL");
        String username = System.getenv("ALIYUN_MYSQL_DB_USER");
        String password = System.getenv("ALIYUN_MYSQL_DB_PASSWORD");
        // 运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 获取SourceTable
        tableEnvironment.executeSql(String.format("""
                CREATE TABLE flink_jdbc_emp (
                  empno INT,
                  ename STRING,
                  job STRING,
                  mgr INT,
                  hiredate DATE,
                  sal DECIMAL(10, 2),
                  comm DECIMAL(10, 2),
                  deptno INT,
                  PRIMARY KEY (empno) NOT ENFORCED
                ) WITH (
                   'connector' = 'jdbc',
                   'url' = 'jdbc:mysql://%s:3306/mydatabase',
                   'table-name' = 'emp',
                   'driver' = 'com.mysql.cj.jdbc.Driver',
                   'username' = '%s',
                   'password' = '%s'
                );
                """, url, username, password));
        // 执行SQL
        tableEnvironment.sqlQuery("select empno,ename,sal from flink_jdbc_emp").execute().print();

    }

}
