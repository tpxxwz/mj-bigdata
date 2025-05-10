package com.mj.trans;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.mj.dto.Event;
import com.mj.dto.User;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 模拟从Kafka读取的数据流（生产环境应使用KafkaSource）
        DataStream<String> sourceStream = env.fromElements(
                "[{\"type\":\"user\",\"id\":1,\"name\":\"Alice\"}," +
                        "{\"type\":\"event\",\"eventId\":1001,\"eventName\":\"login\"}," +
                        "{\"type\":\"user\",\"id\":2,\"name\":\"Bob\"}]"
        );

        // 3. 使用FlatMap解析不同类型的消息
        DataStream<Object> parsedStream = sourceStream.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public void flatMap(String value, Collector<Object> out) throws Exception {
                try {
                    JSONArray jsonArray = JSON.parseArray(value);

                    for (int i = 0; i < jsonArray.size(); i++) {
                        JSONObject jsonObj = jsonArray.getJSONObject(i);
                        String type = jsonObj.getString("type");

                        switch (type) {
                            case "user":
                                User user = jsonObj.toJavaObject(User.class);
                                out.collect(user);
                                break;
                            case "event":
                                Event event = jsonObj.toJavaObject(Event.class);
                                out.collect(event);
                                break;
                            default:
                                System.err.println("Unknown message type: " + type);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error parsing message: " + value);
                    e.printStackTrace();
                }
            }
        });
        // 4. 打印结果
        parsedStream.print();
        // 5. 执行任务
        env.execute("Kafka Message Parser with JSON Processing");
    }
}