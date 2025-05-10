package com.mj.basic4.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaDemo {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 创建Kafka数据源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("mj01:6667")          // Kafka集群地址
                .setTopics("test")                         // 订阅的主题
                .setGroupId("mj-flink-basic")              // 消费者组ID
                .setStartingOffsets(OffsetsInitializer.latest())  // 从最新偏移量开始消费
                .setValueOnlyDeserializer(new SimpleStringSchema())  // 反序列化器
                .build();
        // 3. 从Kafka源创建数据流
        DataStreamSource<String> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),          // 不使用水印策略
                "kafka-source"                            // 数据源名称
        );
        // 4. 打印数据流
        sourceStream.print();
        // 5. 执行作业
        env.execute("Kafka Stream Processing Demo");
    }
}
