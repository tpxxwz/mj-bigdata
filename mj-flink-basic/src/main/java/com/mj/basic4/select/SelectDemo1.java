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
        // 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);

        // 2. 使用 ProcessFunction 分流
        DataStream<String> logs = env.fromElements(
                "INFO: Service started",
                "ERROR: Disk full",
                "INFO: User login"
        );
        // 定义处理逻辑
        DataStream<String> evenStream = logs
                .filter(n -> n.startsWith("ERROR"))  // 过滤偶数
                .name("ERROR");     // 算子命名

        DataStream<String> oddStream = logs
                .filter(n -> n.startsWith("INFO"))  // 过滤偶数
                .name("odd-INFO");      // 算子命名

        // 输出结果
        evenStream.print("错误日志");
        oddStream.print("正常日志");

        env.execute("数据分流示例");
    }
}
