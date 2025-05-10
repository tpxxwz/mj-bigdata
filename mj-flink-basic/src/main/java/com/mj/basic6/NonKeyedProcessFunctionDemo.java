package com.mj.basic6;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.UserWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Collections;

public class NonKeyedProcessFunctionDemo {
    // 定义旁路输出的标签（用于分流金额超过1000的事件）
    private static final OutputTag<UserWindow> HIGH_AMOUNT_TAG = new OutputTag<>("high-amount", TypeInformation.of(UserWindow.class));
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
        // 应用非键控的 ProcessFunction
        SingleOutputStreamOperator<UserWindow> processedStream = parsedStream
                .process(new NonKeyedProcessFunction());

        // 获取旁路输出流（金额超过1000的事件）
        DataStream<UserWindow> highAmountStream = processedStream.getSideOutput(HIGH_AMOUNT_TAG);

        // 打印主输出流和旁路输出流
        processedStream.addSink(new PrintSinkFunction<UserWindow>());
        highAmountStream.addSink(new PrintSinkFunction<UserWindow>());

        env.execute("Non-Keyed ProcessFunction Demo");
    }

    // 自定义 ProcessFunction（非键控流）
    public static class NonKeyedProcessFunction extends ProcessFunction<UserWindow, UserWindow> {
        // 定义状态（非键控流使用 Operator State，此处需谨慎使用）
        private transient ListState<Integer> totalEventCountState;
        @Override
        public void open(OpenContext openContext) {
            // 定义 Operator State 的描述符
            ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>(
                    "total-event-count",
                    Integer.class
            );
            totalEventCountState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(UserWindow event, Context ctx, Collector<UserWindow> out) {
            // 1. 打印事件信息
            System.out.println("处理事件: " + event.getUserId() + ", 金额: " + event.getMoney());

            try {
                // 从 ListState 中获取当前计数（默认是空列表）
                Iterable<Integer> counts = totalEventCountState.get();
                int count = 0;
                if (counts.iterator().hasNext()) {
                    count = counts.iterator().next();
                }
                // 增加计数并更新状态
                count++;
                totalEventCountState.update(Collections.singletonList(count));
            } catch (Exception e) {
                e.printStackTrace();
            }

            // 3. 根据金额分流到旁路输出
            if (event.getMoney() > 1000) {
                ctx.output(HIGH_AMOUNT_TAG, event);
            }

            // 4. 注册一个处理时间定时器（每隔10秒触发）
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 10_000);

            // 5. 主输出流发送事件
            out.collect(event);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<UserWindow> out) {
            // 定时器触发时打印总事件数
            try {
                Iterable<Integer> counts = totalEventCountState.get();
                int count = 0;
                if (counts.iterator().hasNext()) {
                    count = counts.iterator().next();
                }
                System.out.println("定时触发，总事件数: " + count);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
