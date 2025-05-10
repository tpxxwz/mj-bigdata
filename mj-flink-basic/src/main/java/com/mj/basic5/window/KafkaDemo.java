package com.mj.basic5.window;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.Order;
import com.mj.dto.User;
import com.mj.dto.UserWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class KafkaDemo {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 创建Kafka数据源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("mj01:6667")          // Kafka集群地址
                .setTopics("window")                         // 订阅的主题
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
        // 使用 MapFunction 将 JSON 字符串解析为 User 对象
        DataStream<UserWindow> parsedStream = sourceStream.map(new MapFunction<String, UserWindow>() {
            @Override
            public UserWindow map(String value) throws Exception {
                // 这里可以使用 JSON 库（如 Jackson 或 Gson）来解析 JSON 字符串
                return JSON.parseObject(value, UserWindow.class);
            }
        });

        KeyedStream<UserWindow, String> keyedStream = parsedStream.keyBy(wd -> wd.getUserId());
        WindowedStream<UserWindow, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)));
        //WindowedStream<UserWindow, String, TimeWindow> windowedStream = keyedStream.window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10),Duration.ofSeconds(5)));
        //WindowedStream<UserWindow, String, TimeWindow> windowedStream = keyedStream.window(ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(10))) ;
        //WindowedStream<UserWindow, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)));
        //WindowedStream<UserWindow, String, TimeWindow> windowedStream = keyedStream.window(SlidingEventTimeWindows.of(Duration.ofSeconds(10),Duration.ofSeconds(5)));
        //WindowedStream<UserWindow, String, TimeWindow> windowedStream = keyedStream.window(EventTimeSessionWindows.withGap(Duration.ofSeconds(10))) ;

       /* WindowedStream<UserWindow, String, TimeWindow> windowedStream = keyedStream.countWindow(10) ;
        WindowedStream<UserWindow, String, TimeWindow> windowedStream = keyedStream.countWindow(10,3) ;
        WindowedStream<UserWindow, String, TimeWindow> windowedStream = keyedStream.window(GlobalWindows.create())) ;*/
        windowedStream.minBy("eventTime").print();
        // 4. 打印数据流
        //windowedStream.print();
        // 5. 执行作业
        env.execute("Kafka Stream Processing Demo");
    }
}
