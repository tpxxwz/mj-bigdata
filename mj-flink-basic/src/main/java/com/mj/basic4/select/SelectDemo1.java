package com.mj.basic4.select;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class SelectDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建原始数据流
        DataStream<Integer> numbers = env.fromElements(1, 2, 3, 4, 5);

        // 定义处理逻辑
        DataStream<Integer> evenStream = numbers
                .filter(n -> n % 2 == 0)  // 过滤偶数
                .name("even-filter");     // 算子命名

        DataStream<Integer> oddStream = numbers
                .filter(n -> n % 2 != 0)  // 过滤奇数
                .name("odd-filter");      // 算子命名

        // 输出结果
        evenStream.print("偶数流");
        oddStream.print("奇数流");

        env.execute("数据分流示例");
    }
}
