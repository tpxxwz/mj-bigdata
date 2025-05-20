package com.mj.basic4.union;

import com.mj.basic4.union.data.MjEnvReport;
import com.mj.basic4.union.data.MjHumidityEvent;
import com.mj.basic4.union.data.MjTemperatureEvent;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class ConnectDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟温度流（设备A和B的温度数据）
        DataStream<MjTemperatureEvent> tempStream = env.fromElements(
                new MjTemperatureEvent("deviceA", 28.5, 1625000000),
                new MjTemperatureEvent("deviceB", 30.1, 1625000001)
        );

        // 模拟湿度流（设备A和B的湿度数据）
        DataStream<MjHumidityEvent> humidityStream = env.fromElements(
                new MjHumidityEvent("deviceA", 65.2, 1625000000),
                new MjHumidityEvent("deviceB", 58.7, 1625000001)
        );

        // 连接两个流（无需KeyBy，直接合并）
        ConnectedStreams<MjTemperatureEvent, MjHumidityEvent> connectedStreams =
                tempStream.connect(humidityStream);

        // 使用 CoMapFunction 转换数据
        DataStream<MjEnvReport> envReportStream = connectedStreams
                .map(new SensorDataCoMapper());

        envReportStream.print();
        env.execute("Sensor Data Fusion");
    }
    public static class SensorDataCoMapper implements
            CoMapFunction<MjTemperatureEvent, MjHumidityEvent, MjEnvReport> {

        // 处理温度流中的元素
        @Override
        public MjEnvReport map1(MjTemperatureEvent temp) {
            return new MjEnvReport(
                    temp.getDeviceId(),
                    "TEMPERATURE",
                    temp.getTemperature(),
                    temp.getTimestamp()
            );
        }

        // 处理湿度流中的元素
        @Override
        public MjEnvReport map2(MjHumidityEvent humidity) {
            return new MjEnvReport(
                    humidity.getDeviceId(),
                    "HUMIDITY",
                    humidity.getHumidity(),
                    humidity.getTimestamp()
            );
        }
    }
}
