package com.mj.basic4.operator;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.MjSensorReading;
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
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 调用工具类创建 KafkaSource
        // 创建日志数据源
        DataStream<String> sourceStream = env.fromElements(
                "{\"deviceId\":\"devide_001\",\"temperature\":25.0,\"timestamp\":1747561278951}\n"
        );
        // 使用 MapFunction 将 JSON 字符串解析为 User 对象
        DataStream<MjSensorReading> parsedStream = sourceStream.map(new MapFunction<String, MjSensorReading>() {
            @Override
            public MjSensorReading map(String value) throws Exception {
                // 这里可以使用 JSON 库（如 Jackson 或 Gson）来解析 JSON 字符串
                return JSON.parseObject(value, MjSensorReading.class);
            }
        });
        // 打印解析后的用户对象
        parsedStream.print();

        // 执行 Flink 任务
        env.execute("Flink Map Example");
    }
}
