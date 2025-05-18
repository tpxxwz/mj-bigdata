package com.mj.basic4.operator;

import com.alibaba.fastjson2.JSON;
import com.mj.dto.SensorRead;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class FilterDemo2 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 模拟传感器数据源
        DataStream<String> sourceStream = env.fromElements(
                "{\"device\":\"设备1\",\"temperature\":\"38\"}",
                "{\"device\":\"设备2\",\"temperature\":\"55\"}",
                "{\"device\":\"设备3\",\"temperature\":\"37.5\"}"
        );
        // 使用 MapFunction 将 JSON 字符串解析为 User 对象
        DataStream<SensorRead> parsedStream = sourceStream.map(new MapFunction<String, SensorRead>() {
            @Override
            public SensorRead map(String value) throws Exception {
                // 这里可以使用 JSON 库（如 Jackson 或 Gson）来解析 JSON 字符串
                return JSON.parseObject(value, SensorRead.class);
            }
        });
        // 3. 使用filter操作筛选高温数据
        DataStream<SensorRead> alerts = parsedStream.filter(r -> r.getTemperature() > 38.0);
        // 4. 输出高温警报
        alerts.print();
        // 5. 执行作业
        env.execute("High Temperature Alert Job");
    }
}
