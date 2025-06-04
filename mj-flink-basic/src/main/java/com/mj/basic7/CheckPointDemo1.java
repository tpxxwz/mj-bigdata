package com.mj.basic7;

import com.alibaba.fastjson2.JSON;
import com.mj.bean.CheckBean;
import com.mj.bean.WaterMarkData;
import com.mj.utils.TimeConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Random;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class CheckPointDemo1 {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(5000l);
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        env.configure(config);
        // 2. 创建数据生成器源 (生成 CheckBean 对象)
        DataGeneratorSource<CheckBean> dataGeneratorSource =
                new DataGeneratorSource<>(
                        new GeneratorFunction<Long, CheckBean>() {
                            private final Random random = new Random();
                            @Override
                            public CheckBean map(Long value) {
                                // 生成随机用户ID (简化版UUID)
                                String userId = "user_1" ;
                                int money = 100;
                                return new CheckBean(userId, money);
                            }
                        },
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(1), //每秒1条
                        TypeInformation.of(CheckBean.class) // 指定CheckBean类型信息
                );

        // 3. 从数据生成器源创建数据流
        DataStream<CheckBean> stream = env.fromSource(
                dataGeneratorSource,
                WatermarkStrategy.noWatermarks(),
                "checkbean-generator"
        );
        // 按用户ID分组处理金额汇总
        DataStream<String> result = stream.keyBy(CheckBean::getUserId)
                .process(new MoneySumProcessFunction()).uid("Process-1").name("Process-1");
        result.print();
        // 数据写入Kafka
        env.execute("Money Sum Tracking");
    }

    // 自定义处理函数-金额累加
    public static class MoneySumProcessFunction extends KeyedProcessFunction<String, CheckBean, String> {
        // 声明值状态（保存当前用户的金额总和）
        private ValueState<Integer> sumState;

        @Override
        public void open(OpenContext openContext) {
            // 初始化值状态（默认值为0）
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>("moneySum", Integer.class);
            sumState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(CheckBean data, Context ctx, Collector<String> out) throws Exception {
            // 获取当前状态值（若为空则默认为0）
            Integer currentSum = sumState.value() == null ? 0 : sumState.value();
            // 累加新金额
            Integer newSum = currentSum + data.getMoney();
            // 更新状态
            sumState.update(newSum);
            // 输出当前总金额
            out.collect("当前时间："+ TimeConverter.convertLongToDateTime(System.currentTimeMillis()) +
                    ",用户 " + data.getUserId() + " 当前总金额: " + newSum);
        }
    }
}