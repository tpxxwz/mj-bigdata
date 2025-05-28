package com.mj.basic7;

import com.mj.bean.MjSensorReading;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class OperatorBroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 模拟传感器数据流（设备ID,用户ID,温度值,时间戳）
        DataStream<MjSensorReading> sensorDS = env.fromElements(
                new MjSensorReading("devide1",System.currentTimeMillis(),33),
                new MjSensorReading("devide2",System.currentTimeMillis(),70),
                new MjSensorReading("devide3",System.currentTimeMillis(),70)
        );

        // 3. 模拟配置流（动态阈值参数）
        DataStream<String> configDS = env.fromElements(
                "38",  // 初始阈值
                "37"   // 更新后的阈值
        ).setParallelism(1);

        // 4. 定义广播状态描述符
        MapStateDescriptor<String, Integer> broadcastStateDesc =
                new MapStateDescriptor<>(
                        "threshold-config",  // 状态名称
                        Types.STRING,        // Key类型
                        Types.INT            // Value类型
                );

        // 5. 将配置流广播（关键步骤）
        BroadcastStream<String> broadcastConfig = configDS.broadcast(broadcastStateDesc);

        // 6. 连接主数据流和广播流
        BroadcastConnectedStream<MjSensorReading, String> connectedStream =
                sensorDS.connect(broadcastConfig);

        // 7. 处理连接后的流
        DataStream<String> result = connectedStream
                .process(new BroadcastProcessFunction<MjSensorReading, String, String>() {
                    // 处理传感器数据（只能读取广播状态）
                    @Override
                    public void processElement(MjSensorReading reading,
                                               ReadOnlyContext ctx,
                                               Collector<String> out) throws Exception {
                        // 获取只读广播状态
                        ReadOnlyBroadcastState<String, Integer> state =
                                ctx.getBroadcastState(broadcastStateDesc);

                        // 获取阈值（处理初始空值情况）
                        Integer threshold = state.get("temperature");
                        if (threshold == null) {
                            threshold = 0;  // 默认值
                        }

                        // 温度超过阈值报警
                        if (reading.getTemperature() > threshold) {
                            out.collect(String.format("[警报] 设备%s温度%.1f°C超过阈值%d°C",
                                    reading.getDeviceId(),
                                    reading.getTemperature(),
                                    threshold));
                        }
                    }

                    // 处理广播配置（可以修改广播状态）
                    @Override
                    public void processBroadcastElement(String config,
                                                        Context ctx,
                                                        Collector<String> out) throws Exception {
                        // 获取可写广播状态
                        BroadcastState<String, Integer> state =
                                ctx.getBroadcastState(broadcastStateDesc);

                        // 解析并更新阈值配置
                        try {
                            int newThreshold = Integer.parseInt(config);
                            state.put("temperature", newThreshold);
                            out.collect("[配置更新] 新阈值：" + newThreshold);
                        } catch (NumberFormatException e) {
                            out.collect("[错误] 无效配置: " + config);
                        }
                    }
                });

        // 8. 输出结果
        result.print();

        // 9. 执行任务
        env.execute("Broadcast State Demo");
    }
}