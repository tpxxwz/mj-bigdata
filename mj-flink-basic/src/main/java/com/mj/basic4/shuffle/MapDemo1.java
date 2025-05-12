package com.mj.basic4.shuffle;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo1 {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        // 2. 创建输入数据流
        DataStream<String> input = env.fromElements(
                "flink",
                "spark",
                "hadoop"
        );
        // 3. 定义Map转换操作
        DataStream<String> output = input.map(
                new MapFunction<String, String>() {
                    @Override
                    public String map(String value) {
                        return value.toUpperCase();  // 将字符串转换为大写
                    }
                }
        );
        DataStream<String> output1 = output.rescale();
        //output.broadcast()
        // 4. 打印输出结果
        output1.print();
        // 5. 执行作业
        env.execute("String Uppercase Transformation Demo");  // 更符合实际功能的作业名称
    }
}
