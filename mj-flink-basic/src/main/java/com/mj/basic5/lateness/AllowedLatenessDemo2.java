package com.mj.basic5.lateness;

import com.alibaba.fastjson2.JSON;
import com.mj.bean.WaterMarkData;
import com.mj.utils.KafkaUtils;
import com.mj.utils.TimeConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class AllowedLatenessDemo2 {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 定义侧输出流标签（用于捕获超过 allowedLateness 的数据）
        final OutputTag<WaterMarkData> lateOutputTag = new OutputTag<WaterMarkData>("late-data"){};

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

        // 5. 解析JSON数据并分配时间戳和水印
        DataStream<WaterMarkData> parsedStream = sourceStream.map(
                value -> JSON.parseObject(value, WaterMarkData.class)
        );

        WatermarkStrategy<WaterMarkData> orderWatermarkStrategy =
                WatermarkStrategy.<WaterMarkData>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, ts) -> event.getEventTime());

        parsedStream = parsedStream.assignTimestampsAndWatermarks(orderWatermarkStrategy);

        // 6. KeyBy用户ID
        KeyedStream<WaterMarkData, String> keyedStream = parsedStream.keyBy(WaterMarkData::getUserId);

        // 7. 定义窗口、允许延迟及侧输出流
        SingleOutputStreamOperator<String> mainStream = keyedStream
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .allowedLateness(Duration.ofSeconds(5))  // 允许延迟 5 秒
                .sideOutputLateData(lateOutputTag)        // 超时数据发送到侧输出流
                .process(new ProcessWindowFunction<WaterMarkData, String, String, TimeWindow>() {
                    @Override
                    public void process(
                            String key,
                            Context context,
                            Iterable<WaterMarkData> elements,
                            Collector<String> out) {

                        TimeWindow window = context.window();
                        String windowStart = TimeConverter.convertLongToDateTime(window.getStart());
                        String windowEnd = TimeConverter.convertLongToDateTime(window.getEnd());

                        List<WaterMarkData> usersInWindow = new ArrayList<>();
                        elements.forEach(usersInWindow::add);

                        String output = String.format(
                                "\n==== 窗口触发 [%s - %s] ====\n" +
                                        "窗口内数据量: %d 条\n" +
                                        "详细数据: %s\n" +
                                        "触发类型: %s\n" +
                                        "==============================",
                                windowStart, windowEnd, usersInWindow.size(), usersInWindow,
                                context.currentWatermark() >= window.getEnd() ? "延迟触发" : "首次触发"
                        );

                        out.collect(output);
                    }
                });

        // 8. 处理主输出流
        mainStream.print("主输出流");

        // 9. 处理侧输出流（超时数据）
        DataStream<WaterMarkData> lateDataStream = mainStream.getSideOutput(lateOutputTag);
        lateDataStream.map(data -> {
            String lateTime = TimeConverter.convertLongToDateTime(data.getEventTime());
            return String.format("[超时数据] 事件时间: %s, 数据内容: %s", lateTime, data);
        }).print("侧输出流");

        // 10. 执行作业
        env.execute("Window with Late Data Handling Demo");
    }
}
