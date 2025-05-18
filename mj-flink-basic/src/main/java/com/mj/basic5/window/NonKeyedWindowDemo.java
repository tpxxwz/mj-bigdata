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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class NonKeyedWindowDemo {
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
        // 定义非 KeyBy 窗口（滚动时间窗口，5秒）
        parsedStream
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))// 关键API
                .process(new ProcessAllWindowFunction<UserWindow, String, TimeWindow>() {
                    @Override
                    public void process(
                            Context context,
                            Iterable<UserWindow> elements,
                            Collector<String> out) {
                        // 获取窗口时间范围
                        TimeWindow window = context.window();
                        String windowStart = TimeConverter.convertLongToDateTime(window.getStart());
                        String windowEnd = TimeConverter.convertLongToDateTime(window.getEnd());

                        int totalVisits = 0;
                        for (UserWindow element : elements) {
                            totalVisits += 1;
                        }
                        // 构造输出信息
                        String output = String.format(
                                "\n==== 窗口触发 [%s - %s] ====\n" +
                                        "窗口内数据量: %d 条\n" +
                                        "==============================",
                                windowStart,windowEnd,totalVisits);
                        out.collect(output);
                    }
                })
                .print();
        env.execute("Global Window All Demo");
    }
}
