package com.mj.basic5.process;

import com.alibaba.fastjson2.JSON;
import com.mj.bean.WaterMarkData;
import com.mj.utils.KafkaUtils;
import com.mj.utils.TimeConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 码界探索
 * 微信: 252810631
 * 演示 AggregateFunction 与 ProcessWindowFunction 的结合使用
 */

public class WindowAggregateAndProcessDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 创建 Kafka 数据源（调用工具类）
        KafkaSource<String> kafkaSource = KafkaUtils.createKafkaSource(
                "mj01:6667",       // Kafka 集群地址
                "window",          // 订阅主题
                "mj-flink-basic"   // 消费者组ID
        );

        // 3. 从 Kafka 源创建数据流
        DataStreamSource<String> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),  // 禁用水印（处理时间语义）
                "kafka-source"                    // 数据源名称
        );

        // 4. 解析 JSON 数据为 WaterMarkData 对象
        DataStream<WaterMarkData> parsedStream = sourceStream.map(
                value -> JSON.parseObject(value, WaterMarkData.class)
        );

        // 5. 按用户ID进行分区
        KeyedStream<WaterMarkData, String> keyedStream = parsedStream.keyBy(WaterMarkData::getUserId);

        // 6. 窗口分配器 - 10秒的滚动处理时间窗口
        WindowedStream<WaterMarkData, String, TimeWindow> sensorWS = keyedStream
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)));

        // 7. 应用聚合函数和窗口处理函数
        SingleOutputStreamOperator<String> result = sensorWS.aggregate(
                new MyAgg(),    // 增量聚合函数
                new MyProcess() // 全窗口处理函数
        );

        // 8. 打印结果
        result.print();

        // 9. 执行任务
        env.execute("Window Aggregate and Process Demo");
    }

    // ==================================================================
    // 自定义增量聚合函数
    // 输入类型: WaterMarkData
    // 累加器类型: Integer (存储金额累加值)
    // 输出类型: String (将最终累加结果转为字符串)
    // ==================================================================
    public static class MyAgg implements AggregateFunction<WaterMarkData, Integer, String> {
        @Override
        public Integer createAccumulator() {
            System.out.println("[AGG] 创建累加器");
            return 0; // 初始累加值为0
        }

        @Override
        public Integer add(WaterMarkData value, Integer accumulator) {
            System.out.println("[AGG] 处理数据: " + value);
            return accumulator + value.getMoney(); // 累加金额字段
        }

        @Override
        public String getResult(Integer accumulator) {
            System.out.println("[AGG] 生成最终结果: " + accumulator);
            return accumulator.toString(); // 将整型结果转为字符串
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            System.out.println("[AGG] 合并累加器");
            return a + b; // 合并策略：简单相加（会话窗口需要时使用）
        }
    }

    // ==================================================================
    // 自定义全窗口处理函数
    // 输入类型: String (来自聚合函数的输出)
    // 输出类型: String (格式化后的结果字符串)
    // Key类型: String (用户ID)
    // 窗口类型: TimeWindow
    // ==================================================================
    public static class MyProcess extends ProcessWindowFunction<String, String, String, TimeWindow> {
        @Override
        public void process(
                String key,
                Context context,
                Iterable<String> elements, // 这里只会有一个元素（聚合结果）
                Collector<String> out) throws Exception {

            // 获取窗口元数据
            TimeWindow window = context.window();
            String windowStart = TimeConverter.convertLongToDateTime(window.getStart());
            String windowEnd = TimeConverter.convertLongToDateTime(window.getEnd());

            // 获取聚合结果（elements 只有一个元素）
            String aggResult = elements.iterator().next();

            // 构造输出信息
            String output = String.format(
                    "用户 %s 的窗口 [%s - %s)\n" +
                            "聚合金额总计: %s\n" +
                            "--------------------------------",
                    key, windowStart, windowEnd, aggResult
            );

            out.collect(output);
        }
    }
}
