package com.mj.basic6;

import com.alibaba.fastjson2.JSON;
import com.mj.bean.WaterMarkData;
import com.mj.utils.KafkaUtils;
import com.mj.utils.TimeConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ProcessWindowFunctionDemo {
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
        windowedStream.process(new ProcessWindowFunction<WaterMarkData, String, String, TimeWindow>() {
            @Override
            public void process(
                    String key,
                    Context context,
                    Iterable<WaterMarkData> elements,
                    Collector<String> out) {
                // 将窗口内所有商品存入列表
                List<WaterMarkData> items = new ArrayList<>();
                for (WaterMarkData item : elements) {
                    items.add(item);
                }

                // 按销售额降序排序
                items.sort((a, b) -> Double.compare(b.getMoney(), a.getMoney()));

                // 取 Top 3（若不足3个则取全部）
                int topSize = Math.min(3, items.size());
                List<WaterMarkData> top3 = items.subList(0, topSize);
                List<String> result = top3.stream()
                        .map(obj -> String.format("%s|%d", obj.getUserId(), obj.getMoney()))
                        .collect(Collectors.toList());
                // 获取窗口时间范围
                TimeWindow window = context.window();
                String windowStart = TimeConverter.convertLongToDateTime(window.getStart());
                String windowEnd = TimeConverter.convertLongToDateTime(window.getEnd());
                // 构造输出信息
                String output = String.format(
                        "\n==== 窗口触发 [%s - %s] ====\n" +
                                "top3: %s\n" +
                                "==============================",
                        windowStart, windowEnd, result);
                // 输出结果：Tuple3<类别, 窗口时间, Top3列表>
                out.collect(output);
            }
        }).print();
        // 7. 执行作业
        env.execute("Window Data Printing Demo");
    }
}
