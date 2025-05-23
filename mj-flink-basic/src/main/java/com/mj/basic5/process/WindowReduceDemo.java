package com.mj.basic5.process;

import com.alibaba.fastjson2.JSON;
import com.mj.bean.WaterMarkData;
import com.mj.utils.KafkaUtils;
import com.mj.utils.TimeConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;

public class WindowReduceDemo {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
        WindowedStream windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)));
        windowedStream.reduce(new ReduceFunction<WaterMarkData>() {
            @Override
            public WaterMarkData reduce(WaterMarkData currentObj, WaterMarkData newObj) throws Exception {
                WaterMarkData result = new WaterMarkData();
                result.setUserId(currentObj.getUserId());
                result.setMoney(currentObj.getMoney() + newObj.getMoney());
                result.setEventTime(newObj.getEventTime());
                result.setTs(TimeConverter.convertLongToDateTime(newObj.getEventTime()));
                return result;
            }
        }).print();
        // 7. 执行作业
        env.execute("Window Data Printing Demo");
    }
}
