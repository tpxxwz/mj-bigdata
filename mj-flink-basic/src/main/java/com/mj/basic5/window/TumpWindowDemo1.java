package com.mj.basic5.window;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.UserWindow;
import com.mj.utils.TimeConverter;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
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
public class TumpWindowDemo1 {
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
        keyedStream.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .process(new ProcessWindowFunction<UserWindow, String, String, TimeWindow>() {
                    @Override
                    public void process(
                            String key,
                            Context context,
                            Iterable<UserWindow> elements,
                            Collector<String> out) {
                        // 获取窗口时间范围
                        TimeWindow window = context.window();
                        String windowStart = TimeConverter.convertLongToDateTime(window.getStart());
                        String windowEnd = TimeConverter.convertLongToDateTime(window.getEnd());
                        // 收集窗口内所有数据
                        List<UserWindow> usersInWindow = new ArrayList<>();
                        elements.forEach(usersInWindow::add);
                        // 构造输出信息
                        String output = String.format(
                                "\n==== 窗口触发 [%s - %s] ====\n" +
                                        "用户ID: %s\n" +
                                        "窗口内数据量: %d 条\n" +
                                        "详细数据: %s\n" +
                                        "==============================",
                                windowStart, windowEnd, key,
                                usersInWindow.size(), usersInWindow
                        );
                        out.collect(output);
                    }
                })
                .print();

        // 7. 执行作业
        env.execute("Window Data Printing Demo");
    }
}
