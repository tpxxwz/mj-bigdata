package com.mj.basic4.operator;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class MapDemo2 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 模拟从 Kafka 读取的数据流
        DataStream<String> sourceStream = env.fromElements(
                "{\"userId\":1,\"name\":\"Alice\"}",
                "{\"userId\":2,\"name\":\"Bob\"}",
                "{\"userId\":3,\"name\":\"Charlie\"}"
        );
        // 使用 MapFunction 将 JSON 字符串解析为 User 对象
        DataStream<User> parsedStream = sourceStream.map(new MapFunction<String, User>() {
            @Override
            public User map(String value) throws Exception {
                // 这里可以使用 JSON 库（如 Jackson 或 Gson）来解析 JSON 字符串
                return JSON.parseObject(value, User.class);
            }
        });
        // 打印解析后的用户对象
        parsedStream.print();

        // 执行 Flink 任务
        env.execute("Flink Map Example");
    }
}
