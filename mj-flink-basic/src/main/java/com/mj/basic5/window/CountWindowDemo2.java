package com.mj.basic5.window;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.UserWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class CountWindowDemo2 {
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
        //WindowedStream<UserWindow, String, TimeWindow> windowedStream = keyedStream.window(ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(10))) ;

        // 6. 定义窗口并处理窗口数据
        keyedStream.countWindow(4,2)  // 每 10 条数据触发窗口
                .process(new ProcessWindowFunction<UserWindow, String, String, GlobalWindow>() {
                    @Override
                    public void process(
                            String key,
                            Context context,
                            Iterable<UserWindow> elements,
                            Collector<String> out) {

                        // 获取窗口元数据（计数窗口无时间范围）
                        GlobalWindow window = context.window();
                        long count = elements.spliterator().getExactSizeIfKnown();

                        // 构造输出信息（移除时间相关字段）
                        String output = String.format(
                                "\n==== 窗口触发 [Count: %d] ====\n" +
                                        "用户ID: %s\n" +
                                        "窗口内数据量: %d 条\n" +
                                        "详细数据: %s\n" +
                                        "==============================",
                                window.hashCode(), key, count, elements
                        );

                        out.collect(output);
                    }
                })
                .print();
        // 7. 执行作业
        env.execute("Window Data Printing Demo");
    }
}
