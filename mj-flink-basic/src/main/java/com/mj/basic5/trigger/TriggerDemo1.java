package com.mj.basic5.trigger;

import com.alibaba.fastjson2.JSON;
import com.mj.bean.WaterMarkData;
import com.mj.utils.KafkaUtils;
import com.mj.utils.TimeConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
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
public class TriggerDemo1 {
    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 侧输出流标签定义
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

        // 4. 解析数据并分配水印
        DataStream<WaterMarkData> parsedStream = sourceStream.map(
                value -> JSON.parseObject(value, WaterMarkData.class)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterMarkData>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, ts) -> event.getEventTime())
        );

        // 5. KeyBy 用户ID
        KeyedStream<WaterMarkData, String> keyedStream = parsedStream.keyBy(WaterMarkData::getUserId);

        // 6. 定义窗口并配置自定义触发器
        SingleOutputStreamOperator<String> mainStream = keyedStream
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .trigger(new CustomTrigger())  // 使用自定义触发器
                .allowedLateness(Duration.ofSeconds(5))
                .sideOutputLateData(lateOutputTag)
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

                        String triggerType = context.currentWatermark() >= window.getEnd()
                                ? "延迟触发"
                                : (usersInWindow.size() >= 3 ? "计数触发" : "时间触发");

                        String output = String.format(
                                "\n==== 窗口 [%s - %s] ====\n" +
                                        "数据量: %d 条\n" +
                                        "触发类型: %s\n" +
                                        "数据明细: %s\n" +
                                        "========================",
                                windowStart, windowEnd, usersInWindow.size(), triggerType, usersInWindow
                        );

                        out.collect(output);
                    }
                });

        // 7. 处理主流和侧输出流
        mainStream.print("主输出流");
        mainStream.getSideOutput(lateOutputTag)
                .map(data -> "[超时数据] " + data)
                .print("侧输出流");

        env.execute("Window with Custom Trigger Demo");
    }

    // 自定义触发器实现
    public static class CustomTrigger extends Trigger<WaterMarkData, TimeWindow> {
        private final int countThreshold = 3; // 提前触发的数据量阈值

        // 定义计数器状态描述符
        private  final ValueStateDescriptor<Integer> stateDesc =
                new ValueStateDescriptor<>("countState", Integer.class);
        @Override
        public TriggerResult onElement(
                WaterMarkData element,
                long timestamp,
                TimeWindow window,
                TriggerContext ctx) throws Exception {

            // 注册窗口结束时间的事件时间触发器
            ctx.registerEventTimeTimer(window.getEnd());

            // 如果窗口内数据量达到阈值，立即触发
            if (ctx.getPartitionedState(stateDesc).value() >= countThreshold) {
                return TriggerResult.FIRE;
            } else {
                // 更新计数器
                ValueState<Integer> countState = ctx.getPartitionedState(stateDesc);
                countState.update(countState.value() == null ? 1 : countState.value() + 1);
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onEventTime(
                long time,
                TimeWindow window,
                TriggerContext ctx) throws Exception {
             return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(
                long time,
                TimeWindow window,
                TriggerContext ctx) throws Exception {

            // 不需要处理时间触发
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(stateDesc).clear();
        }
        @Override
        public boolean canMerge() {
            return true;
        }


    }
}
