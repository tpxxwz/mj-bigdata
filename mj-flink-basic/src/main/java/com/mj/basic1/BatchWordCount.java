package com.mj.basic1;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 批处理单词计数示例（使用Flink流处理API模拟批处理）
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境（虽然处理批数据，但使用流API）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 创建模拟数据源（实际应用中可以替换为文件/消息队列等）
        DataStream<String> sourceStream = env.fromElements(
                "Hello World Hello Flink",
                "Flink is World is beautiful"
        );

        // 3. 数据转换：拆分句子为单词元组（单词, 1）
        DataStream<Tuple2<String, Integer>> flatMap = sourceStream
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    // 使用正则表达式按空格拆分单词
                    for (String word : line.split("\\s+")) {
                        // 收集每个单词并初始化为计数1
                        out.collect(new Tuple2<>(word, 1));
                    }
                })
                // 显式指定返回类型（Flink类型系统需要类型信息）
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        // 4. 按键分组：按单词进行分组
        KeyedStream<Tuple2<String, Integer>, String> keyBy = flatMap
                // 使用元组的第一个字段（单词）作为分组键
                .keyBy(value -> value.f0);

        // 5. 聚合计算：对每个单词的计数求和
        DataStream<Tuple2<String, Integer>> sumStream = keyBy
                // 对每个分组的第二个字段（即计数）进行累加
                .sum(1);

        // 6. 结果输出：打印处理结果到控制台
        sumStream.print();

        // 7. 触发任务执行（流处理任务需要显式执行）
        env.execute("Batch Word Count Example");
    }
}