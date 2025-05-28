package com.mj.basic7;

import com.mj.bean.MjSensorReading;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputDemo {
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

        // 定义一个侧输出标签（异常数据流）
        OutputTag<String> warnTag = new OutputTag<>("warn", Types.STRING);
        SingleOutputStreamOperator<MjSensorReading> process = sensorDS.keyBy(sensor -> sensor.getDeviceId())
                .process(
                        new KeyedProcessFunction<String, MjSensorReading, MjSensorReading>() {
                            @Override
                            public void processElement(MjSensorReading value, Context ctx, Collector<MjSensorReading> out) throws Exception {
                                // 使用侧输出流告警
                                if (value.getTemperature() > 34) {
                                    ctx.output(warnTag, "设备"+value.getDeviceId()+",当前温度=" + value.getTemperature() + ",大于阈值30！！！");
                                }
                                // 主流正常 发送数据
                                out.collect(value);
                            }
                        }
                );

        process.print("主流");
        process.getSideOutput(warnTag).printToErr("warn");

        // 9. 执行任务
        env.execute("Broadcast State Demo");
    }
}
