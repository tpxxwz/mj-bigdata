package com.mj.basic4.operator;

import com.alibaba.fastjson2.JSON;
import com.mj.bean.MjSensorReading;
import com.mj.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class MapDemo1 {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 调用工具类创建 KafkaSource
        KafkaSource<String> kafkaSource = KafkaUtils.createKafkaSource(
                "mj01:6667",       // Kafka 集群地址
                "window",          // 订阅的主题
                "mj-flink-basic"   // 消费者组ID
        );
        // 3. 从 Kafka 源创建数据流
        DataStreamSource<String> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),  // 水印策略
                "kafka-source"                    // 数据源名称
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
