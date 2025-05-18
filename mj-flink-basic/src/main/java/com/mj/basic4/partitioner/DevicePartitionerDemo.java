package com.mj.basic4.partitioner;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.DeviceData;
import com.mj.dto.UserWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class DevicePartitionerDemo {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 创建Kafka数据源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("mj01:6667")
                .setTopics("window")
                .setGroupId("mj-flink-basic")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // 3. 从Kafka源创建数据流
        DataStreamSource<String> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );
        // 4. 解析JSON数据
        DataStream<DeviceData> parsedStream = sourceStream.map(
                value -> JSON.parseObject(value, DeviceData.class)
        );
        // 模拟设备数据流（设备ID, 温度值）
        DataStream<DeviceData> dataStream = parsedStream.partitionCustom(new DeviceTypePartitioner(),
                        (KeySelector<DeviceData, String>) DeviceData::getDeviceId);
        //output.broadcast()
        // 4. 打印输出结果
        dataStream.print();
        // 5. 执行作业
        env.execute("String Uppercase Transformation Demo");  // 更符合实际功能的作业名称
    }
}
