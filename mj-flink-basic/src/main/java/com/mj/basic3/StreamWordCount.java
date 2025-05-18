package com.mj.basic3;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT.key(), "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        //设置并行度为1
        env.setParallelism(2);
        //设置禁用合并链
        //env.disableOperatorChaining();
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
        DataStream<Tuple2<String,Integer>> parsedStream = sourceStream
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    for (String word : line.split("\\s+")) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        // 5. KeyBy 单词
        KeyedStream<Tuple2<String, Integer>, String> keyBy =parsedStream
                .keyBy(value -> value.f0);

        // 6.sum 聚合
        DataStream<Tuple2<String, Integer>> sumStream = keyBy
                .sum(1);

        //打印
        sumStream.print();

        // 7. 执行作业
        env.execute("Window Data Printing Demo");
    }
}