package com.mj.basic4.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class SinkDemo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 模拟订单数据流（实际从Kafka等Source读取）
        DataStream<String> orderStream = env.fromElements(
                "{\"order_id\":\"1001\", \"amount\":99.9, \"event_time\":\"2023-10-01 08:15:00\"}",
                "{\"order_id\":\"1002\", \"amount\":199.9, \"event_time\":\"2023-10-01 08:25:00\"}",
                "{\"order_id\":\"1003\", \"amount\":299.9, \"event_time\":\"2023-10-01 09:05:00\"}"
        );

        // 定义文件Sink（路径示例为HDFS，本地路径可替换为"file:///path/to/output"）
        FileSink<String> fileSink = FileSink
                .forRowFormat(new Path("D:/mj/orders"), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd/HH")) // 按小时分桶
                .withRollingPolicy( // 文件滚动策略
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(10 * 60 * 1000)  // 10分钟无新数据则滚动
                                .withMaxPartSize(128 * 1024 * 1024)    // 128MB 滚动
                                .build()
                )
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("orders")  // 文件前缀
                        .withPartSuffix(".log")    // 文件后缀
                        .build())
                .build();

        // 数据流写入Sink
        orderStream.sinkTo(fileSink).name("OrderFileSink");

        env.execute("Streaming File Sink Example");
    }
}
