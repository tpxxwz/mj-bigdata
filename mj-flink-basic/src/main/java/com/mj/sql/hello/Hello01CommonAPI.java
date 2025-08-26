package com.mj.sql.hello;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// 创建运行环境
public class Hello01CommonAPI {

    public static void main(String[] args) {
        // 创建环境配置【流批可以自己选择】
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);
        // 直接创建流环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(environment);
    }

}
