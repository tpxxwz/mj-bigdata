package com.mj.basic4.union;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.OrderInfo;
import com.mj.dto.UserWindow;
import com.mj.utils.TimeConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class UnionDemo {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 创建Kafka数据源
        KafkaSource<String> offkafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("mj01:6667")
                .setTopics("off_topic")
                .setGroupId("mj-flink-basic")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // 2. 创建Kafka数据源
        KafkaSource<String> onkafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("mj01:6667")
                .setTopics("on_topic")
                .setGroupId("mj-flink-basic")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // 3. 从Kafka源创建数据流
        DataStreamSource<String> offOrderStream = env.fromSource(
                offkafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );

        DataStreamSource<String> onOrderStream = env.fromSource(
                onkafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );

        DataStream<String> unionStream = offOrderStream.union(onOrderStream);
        // 4. 解析JSON数据
        DataStream<OrderInfo> parsedStream = unionStream.map(
                value -> JSON.parseObject(value, OrderInfo.class)
        );
        // 5. KeyBy 订单ID
        KeyedStream<OrderInfo, String> keyedStream = parsedStream.keyBy(OrderInfo::getOrderId);
        // 统计每分钟订单金额
        keyedStream
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .sum("amount").print();

        // 7. 执行作业
        env.execute("Window Data Printing Demo");
    }
}
