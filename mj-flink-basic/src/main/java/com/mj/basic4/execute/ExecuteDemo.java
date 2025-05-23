package com.mj.basic4.execute;

import com.mj.bean.MjOrderInfo;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class ExecuteDemo {
    public static void main(String[] args) throws Exception {
        // 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        // 模拟输入数据流（实际场景可能来自Kafka/Socket等）
        DataStream<MjOrderInfo> orders = env.fromElements(
                new MjOrderInfo("order1","user1", 199, System.currentTimeMillis()),
                new MjOrderInfo("order2","user2", 299, System.currentTimeMillis()),
                new MjOrderInfo("order11","user1", 599, System.currentTimeMillis()),
                new MjOrderInfo("order22","user2", 99, System.currentTimeMillis()),
                new MjOrderInfo("order111","user2", 899, System.currentTimeMillis())
        );
        orders.print();
        // 5. 执行作业
        JobExecutionResult result =  env.execute("ExecuteDemo Demo");
        System.out.println(result.getNetRuntime());
    }
}
