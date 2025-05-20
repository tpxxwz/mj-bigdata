package com.mj.basic4.partitioner;

import com.alibaba.fastjson2.JSON;
import com.mj.basic4.partitioner.data.MjDeviceData;
import com.mj.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
        env.setParallelism(3);
        // 2. 调用工具类创建 KafkaSource
        KafkaSource<String> kafkaSource = KafkaUtils.createKafkaSource(
                "mj01:6667",       // Kafka 集群地址
                "window",          // 订阅的主题
                "mj-flink-basic"   // 消费者组ID
        );
        // 3. 从Kafka源创建数据流
        DataStreamSource<String> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );
        // 4. 解析JSON数据
        DataStream<MjDeviceData> parsedStream = sourceStream.map(
                value -> JSON.parseObject(value, MjDeviceData.class)
        );
        // 模拟设备数据流（设备ID, 温度值）
        DataStream<MjDeviceData> dataStream = parsedStream.partitionCustom(new DeviceTypePartitioner(),
                        (KeySelector<MjDeviceData, String>) MjDeviceData::getDeviceId);
        // 4. 打印输出结果
        dataStream.print();
        // 5. 执行作业
        env.execute("String Uppercase Transformation Demo");  // 更符合实际功能的作业名称
    }
}
