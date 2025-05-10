package com.mj.basic5.window;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.UserWindow;
import com.mj.utils.TimeConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;

public class WindowAggregateDemo {
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
        windowedStream.aggregate(new AggregateFunction<UserWindow, Tuple2<Long, Long>,Long> () {
            @Override
            public Tuple2<Long, Long> createAccumulator() {
                System.out.println("创建累加器");
                return  Tuple2.of(0L, 0L); // 初始总和和计数;
            }

            @Override
            public Tuple2<Long, Long> add(UserWindow value, Tuple2<Long, Long> acc) {
                return Tuple2.of(acc.f0 + value.getMoney(), acc.f1 + 1);
            }
            @Override
            public Long getResult(Tuple2<Long, Long> acc) {
                return acc.f0 / acc.f1; // 计算平均值
            }
            @Override
            public Tuple2<Long, Long> merge(Tuple2<Long, Long> var1, Tuple2<Long, Long> var2){
                return null;
            }

        }).print();
        // 7. 执行作业
        env.execute("Window Data Printing Demo");
    }
}
