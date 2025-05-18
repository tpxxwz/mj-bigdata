package com.mj.basic5.window;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.UserWindow;
import com.mj.utils.TimeConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class WindowSumMinMaxDemo {
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
        DataStream<UserWindow> parsedStream = sourceStream.map(
                value -> JSON.parseObject(value, UserWindow.class)
        );
        // 5. KeyBy用户ID
        KeyedStream<UserWindow, String> keyedStream = parsedStream.keyBy(UserWindow::getUserId);
        // 6. 定义窗口并处理窗口数据
        WindowedStream windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)));
        windowedStream.max("money").print("max");
        windowedStream.min("money").print("min");
        windowedStream.maxBy("money").print("maxBy");
        windowedStream.minBy("money").print("minBy");
        windowedStream.sum("money").print("sum");

        // 7. 执行作业
        env.execute("Window Data Printing Demo");
    }
}
