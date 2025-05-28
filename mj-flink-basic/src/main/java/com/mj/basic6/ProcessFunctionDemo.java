package com.mj.basic6;

import com.mj.bean.Alert;
import com.mj.bean.MjSensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class ProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟传感器数据流
        DataStream<MjSensorReading> sensorStream = env.fromElements(
                new MjSensorReading("sensor1", 98, System.currentTimeMillis()),
                new MjSensorReading("sensor2", 105, System.currentTimeMillis()),
                new MjSensorReading("sensor3", 120, System.currentTimeMillis())
        );

        sensorStream.process(new TemperatureAlertFunction()).print();

        env.execute("Temperature Monitoring");
    }

    public static class TemperatureAlertFunction extends ProcessFunction<MjSensorReading, Alert> {
        @Override
        public void processElement(MjSensorReading data, Context ctx, Collector<Alert> out) {
            if (data.getTemperature() > 100) {
                out.collect(new Alert("设备过热: " + data.getDeviceId(), data.getTimestamp()));
            }
        }
    }

}
