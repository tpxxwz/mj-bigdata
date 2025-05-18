package com.mj.basic4.select;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class SelectDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1. 定义侧输出标签
        OutputTag<String> infoTag = new OutputTag<String>("info") {};
        OutputTag<String> errorTag = new OutputTag<String>("error") {};

        // 2. 使用 ProcessFunction 分流
        DataStream<String> logs = env.fromElements(
                "INFO: Service started",
                "ERROR: Disk full",
                "INFO: User login"
        );

        SingleOutputStreamOperator<String> mainStream = logs
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String log, Context ctx, Collector<String> out) {
                        if (log.startsWith("INFO:")) {
                            ctx.output(infoTag, log);  // 发送到 INFO 侧流
                        } else if (log.startsWith("ERROR:")) {
                            ctx.output(errorTag, log); // 发送到 ERROR 侧流
                        }
                        out.collect(log); // 主输出流（可选）
                    }
                });

        // 3. 获取侧输出流
        DataStream<String> infoStream = mainStream.getSideOutput(infoTag);
        DataStream<String> errorStream = mainStream.getSideOutput(errorTag);

        infoStream.print("INFO日志");
        errorStream.print("ERROR日志");

        env.execute("数据分流示例");
    }
}
