package com.mj.basic4.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class DataGenDemo {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 创建数据生成器源
        DataGeneratorSource<String> dataGeneratorSource =
                new DataGeneratorSource<>(
                        new GeneratorFunction<Long, String>() {
                            @Override
                            public String map(Long value) throws Exception {
                                return "Number:" + value;  // 生成格式为 "Number:value" 的字符串
                            }
                        },
                        Long.MAX_VALUE,                   // 生成数据的最大数量
                        RateLimiterStrategy.perSecond(10), // 限速策略：每秒10条
                        Types.STRING                      // 输出数据类型
                );

        // 3. 从数据生成器源创建数据流
        DataStream<String> stream = env.fromSource(
                dataGeneratorSource,
                WatermarkStrategy.noWatermarks(),     // 不使用水印策略
                "datagenerator"                       // 数据源名称
        );
        // 4. 打印数据流
        stream.print();
        // 5. 执行作业
        env.execute("Data Generator Demo");       // 更符合实际功能的作业名称
    }
}
