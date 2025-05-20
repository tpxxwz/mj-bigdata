package com.mj.basic4.operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class FilterDemo3 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建日志数据源
        DataStream<String> logStream = env.fromElements(
                "INFO: User logged in",
                "ERROR: Database connection failed",
                "WARNING: Low disk space",
                "ERROR: User authentication failed"
        );
        // 筛选出包含 "ERROR" 的日志记录
        DataStream<String> errorLogs = logStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.contains("ERROR");
            }
        });
        // 打印结果
        errorLogs.print();
        // 执行任务
        env.execute("Log Filter Example");
    }
}
