package com.mj.basic6;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DeviceTemperatureMonitor {
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
        DataStream<SensorReading> parsedStream = sourceStream.map(
                value -> JSON.parseObject(value, SensorReading.class)
        );
        KeyedStream<SensorReading, String> keyedStream = parsedStream.keyBy(SensorReading::getDeviceId);
        DataStream<String> alerts = keyedStream.process(new TemperatureAlertFunction());  // 处理逻辑
        alerts.print();
        env.execute("Device Temperature Monitoring");
    }
    // 温度报警处理函数
    public static class TemperatureAlertFunction
            extends KeyedProcessFunction<String, SensorReading, String> {

        // 定义状态描述符
        private transient ValueState<Double> lastTempState;
        private transient ValueState<Long> timerState;

        @Override
        public void open(OpenContext openContext) {
            // 初始化上次温度状态
            ValueStateDescriptor<Double> lastTempDesc = new ValueStateDescriptor<>(
                    "lastTemperature",
                    TypeInformation.of(Double.class)
            );
            lastTempState = getRuntimeContext().getState(lastTempDesc);

            // 初始化定时器状态
            ValueStateDescriptor<Long> timerDesc = new ValueStateDescriptor<>(
                    "timerState",
                    TypeInformation.of(Long.class)
            );
            timerState = getRuntimeContext().getState(timerDesc);
        }

        @Override
        public void processElement(SensorReading reading,
                                   Context ctx,
                                   Collector<String> out) throws Exception {
            // 获取上次温度（可能为null）
            Double lastTemp = lastTempState.value();
            Long currentTimer = timerState.value();

            // 温度上升逻辑
            if (lastTemp == null || reading.getTemperature() > lastTemp) {
                // 注册10秒后的处理时间定时器
                long timerTime = ctx.timerService().currentProcessingTime() + 10000;
                ctx.timerService().registerProcessingTimeTimer(timerTime);

                // 更新状态
                timerState.update(timerTime);
            } else {
                // 温度未上升则取消定时器
                if (currentTimer != null) {
                    ctx.timerService().deleteProcessingTimeTimer(currentTimer);
                }
                timerState.clear();
            }

            // 更新最新温度
            lastTempState.update(reading.getTemperature());
        }

        @Override
        public void onTimer(long timestamp,
                            OnTimerContext ctx,
                            Collector<String> out) throws Exception {
            // 触发报警
            out.collect("[ALERT] Device " + ctx.getCurrentKey()
                    + ": Temperature continuously rising for 10 seconds!");
            timerState.clear();
        }
    }
}
