package com.mj.basic6;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.UserWindow;
import com.mj.utils.TimeConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ProcessAllWindowFunctionDemo {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 创建 Kafka 数据源（与原代码一致）
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("mj01:6667")
                .setTopics("window")
                .setGroupId("mj-flink-basic")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 3. 从 Kafka 源创建数据流
        DataStreamSource<String> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );

        // 4. 解析 JSON 数据（与原代码一致）
        DataStream<UserWindow> parsedStream = sourceStream.map(
                value -> JSON.parseObject(value, UserWindow.class)
        );

        // 5. 定义全局窗口（无需 KeyBy）
        AllWindowedStream<UserWindow, TimeWindow> allWindowedStream = parsedStream
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)));

        // 6. 处理全局窗口数据
        allWindowedStream.process(new ProcessAllWindowFunction<UserWindow, String, TimeWindow>() {
            @Override
            public void process(
                    Context context,
                    Iterable<UserWindow> elements,
                    Collector<String> out) {
                // 将窗口内所有用户事件存入列表
                List<UserWindow> allUsers = new ArrayList<>();
                for (UserWindow user : elements) {
                    allUsers.add(user);
                }

                // 按销售额降序排序（全局排序）
                allUsers.sort((a, b) -> Double.compare(b.getMoney(), a.getMoney()));

                // 取全局 Top3（若不足3个则取全部）
                int topSize = Math.min(3, allUsers.size());
                List<UserWindow> globalTop3 = allUsers.subList(0, topSize);
                List<String> result = globalTop3.stream()
                        .map(obj -> String.format("%s|%d", obj.getUserId(), obj.getMoney()))
                        .collect(Collectors.toList());

                // 获取窗口时间范围
                TimeWindow window = context.window();
                String windowStart = TimeConverter.convertLongToDateTime(window.getStart());
                String windowEnd = TimeConverter.convertLongToDateTime(window.getEnd());

                // 构造输出信息
                String output = String.format(
                        "\n==== 全局窗口触发 [%s - %s] ====\n" +
                                "全局Top3: %s\n" +
                                "==============================",
                        windowStart, windowEnd, result);

                out.collect(output);
            }
        }).print();

        // 7. 执行作业
        env.execute("Global Window Top3 Demo");
    }
}
