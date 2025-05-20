package com.mj.basic4.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class SocketDemo {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 创建Socket数据源，监听localhost的9000端口
        DataStream<String> stream = env.socketTextStream(
                "mj01",  // 主机名
                9000       // 端口号
        );
        // 3. 打印数据流
        stream.print();
        // 4. 异步执行作业
        env.executeAsync("Socket Stream Processing Demo");
    }
}
