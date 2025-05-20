package com.mj.basic4.union;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.MjOrderInfo;
import com.mj.dto.OrderInfo;
import com.mj.dto.UserWindow;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 调用工具类创建 KafkaSource
        KafkaSource<String> offkafkaSource = KafkaUtils.createKafkaSource(
                "mj01:6667",       // Kafka 集群地址
                "off_topic",          // 订阅的主题
                "mj-flink-basic"   // 消费者组ID
        );
        // 2. 创建Kafka数据源
        KafkaSource<String> onkafkaSource = KafkaUtils.createKafkaSource(
                "mj01:6667",       // Kafka 集群地址
                "on_topic",          // 订阅的主题
                "mj-flink-basic"   // 消费者组ID
        );
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
        DataStream<MjOrderInfo> parsedStream = unionStream.map(
                value -> JSON.parseObject(value, MjOrderInfo.class)
        );
        parsedStream.print();
        // 7. 执行作业
        env.execute("Window Data Printing Demo");
    }
}
