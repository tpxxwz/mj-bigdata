package com.mj.basic8;

import com.mj.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author 码界探索
 * 微信: 252810631 
 * @desc 版权所有，请勿外传
 */
public class LoginSecurityMonitor {
        public static void main(String[] args) throws Exception {
            // 1. 创建流处理执行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // 2. 定义水印策略（处理事件时间和乱序事件）
            WatermarkStrategy<LoginEvent> loginWatermarkStrategy =
                    WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                            .withTimestampAssigner((event, ts) -> event.getTimestamp());  // 从事件中提取时间戳

            // 3. 创建模拟登录事件流（实际生产环境应替换为Kafka/Socket等数据源）
            DataStream<LoginEvent> loginStream = env.fromElements(
                    new LoginEvent("user1", "fail", "2025-05-29 15:00:00"),
                    new LoginEvent("user1", "fail", "2025-05-29 15:00:02"),
                    new LoginEvent("user2", "success", "2025-05-29 15:00:00"),
                    new LoginEvent("user1", "fail", "2025-05-29 15:00:05"),  // 10秒内连续3次失败
                    new LoginEvent("user3", "fail", "2025-05-29 15:00:00"),
                    new LoginEvent("user3", "fail", "2025-05-29 15:00:02"),
                    new LoginEvent("user3", "fail", "2025-05-29 15:00:03")
            ).assignTimestampsAndWatermarks(loginWatermarkStrategy);  // 应用水印策略

            // 4. 定义CEP检测模式：10秒内连续3次登录失败
            Pattern<LoginEvent, ?> loginPattern = Pattern.<LoginEvent>begin("first")  // 第一次失败
                    .where(new SimpleCondition<LoginEvent>() {
                        @Override
                        public boolean filter(LoginEvent event) {
                            return "fail".equals(event.getStatus());  // 筛选失败事件
                        }
                    })
                    .next("second")  // 紧邻的下一次失败
                    .where(new SimpleCondition<LoginEvent>() {
                        @Override
                        public boolean filter(LoginEvent event) {
                            return "fail".equals(event.getStatus());
                        }
                    })
                    .next("third")  // 紧邻的第三次失败
                    .where(new SimpleCondition<LoginEvent>() {
                        @Override
                        public boolean filter(LoginEvent event) {
                            return "fail".equals(event.getStatus());
                        }
                    })
                    .within(Duration.ofSeconds(10));  // 时间窗口限制为10秒

            // 5. 应用模式到数据流
            PatternStream<LoginEvent> patternStream = CEP.pattern(
                    loginStream.keyBy(event -> event.getUserId()),  // 按用户ID分组（相同用户的事件进入同一处理管道）
                    loginPattern
            );

            // 6. 处理匹配的模式事件（检测到连续三次失败时触发）
            DataStream<String> alerts = patternStream.process(
                    new PatternProcessFunction<LoginEvent, String>() {
                        @Override
                        public void processMatch(
                                Map<String, List<LoginEvent>> match,  // 匹配到的事件序列（按模式名称分组）
                                Context ctx,  // 上下文信息
                                Collector<String> out) {  // 输出收集器

                            // 从匹配结果中提取具体事件
                            LoginEvent first = match.get("first").get(0);   // 第一次失败事件
                            LoginEvent second = match.get("second").get(0); // 第二次失败事件
                            LoginEvent third = match.get("third").get(0);   // 第三次失败事件

                            // 生成告警信息（计算时间间隔并格式化输出）
                            String alertMsg = String.format(
                                    "安全告警！用户 %s 在 %d 秒内连续登录失败3次。失败序列: [%s, %s, %s]",
                                    first.getUserId(),  // 注意修正：原代码此处有误，应为getUserId()
                                    (third.getTimestamp() - first.getTimestamp()) / 1000,  // 转换为秒
                                    first.getTs(), second.getTs(), third.getTs()  // 事件时间戳
                            );
                            out.collect(alertMsg);
                        }
                    });

            // 7. 输出告警（生产环境应写入Kafka/数据库/告警系统）
            alerts.print();  // 控制台打印测试

            // 8. 启动作业
            env.execute("Login Security Monitor");
        }
    }