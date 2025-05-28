package com.mj.basic6;

import com.alibaba.fastjson2.JSON;
import com.mj.bean.WaterMarkData;
import com.mj.utils.KafkaUtils;
import com.mj.utils.TimeConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class ProcessAllWindowFunctionDemo {
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
        // 5. 定义全局窗口（无需 KeyBy）
        AllWindowedStream<WaterMarkData, TimeWindow> allWindowedStream = parsedStream
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)));

        // 6. 处理全局窗口数据
        allWindowedStream.process(new ProcessAllWindowFunction<WaterMarkData, String, TimeWindow>() {
            @Override
            public void process(
                    Context context,
                    Iterable<WaterMarkData> elements,
                    Collector<String> out) {
                // 将窗口内所有用户事件存入列表
                List<WaterMarkData> allUsers = new ArrayList<>();
                for (WaterMarkData user : elements) {
                    allUsers.add(user);
                }

                // 按销售额降序排序（全局排序）
                allUsers.sort((a, b) -> Double.compare(b.getMoney(), a.getMoney()));

                // 取全局 Top3（若不足3个则取全部）
                int topSize = Math.min(3, allUsers.size());
                List<WaterMarkData> globalTop3 = allUsers.subList(0, topSize);
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
