package com.mj.basic5.window;

import com.alibaba.fastjson2.JSON;
import com.mj.basic5.data.WaterMarkData;
import com.mj.utils.KafkaUtils;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class TumpWindowDemo1 {
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
        // 4. 解析JSON数据
        DataStream<WaterMarkData> parsedStream = sourceStream.map(
                value -> JSON.parseObject(value, WaterMarkData.class)
        );
        // 5. KeyBy用户ID
        KeyedStream<WaterMarkData, String> keyedStream = parsedStream.keyBy(WaterMarkData::getUserId);
        // 6. 定义窗口并处理窗口数据
        keyedStream.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .process(new ProcessWindowFunction<WaterMarkData, String, String, TimeWindow>() {
                    @Override
                    public void process(
                            String key,
                            Context context,
                            Iterable<WaterMarkData> elements,
                            Collector<String> out) {
                        // 获取窗口时间范围
                        TimeWindow window = context.window();
                        String windowStart = TimeConverter.convertLongToDateTime(window.getStart());
                        String windowEnd = TimeConverter.convertLongToDateTime(window.getEnd());
                        // 收集窗口内所有数据
                        List<WaterMarkData> usersInWindow = new ArrayList<>();
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
