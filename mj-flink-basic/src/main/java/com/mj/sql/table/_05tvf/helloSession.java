package com.mj.sql.table._05tvf;

import com.mj.utils.DateUtils;
import com.mj.utils.KafkaUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * SESSION(TABLE data [PARTITION BY(keycols, ...)], DESCRIPTOR(timecol), gap)
 * data：拥有时间属性列的表。
 * keycols：列描述符，决定会话窗口应该使用哪些列来分区数据。
 * timecol：列描述符，决定数据的哪个时间属性列应该映射到窗口。
 * gap：两个事件被认为属于同一个会话窗口的最大时间间隔。
 * 月榜 周榜 隔一段时间要从0开始算 算也要一段一段时间算
 */
public class helloSession {

    public static void main(String[] args) {
        String topic = "kafka_tvf_session_topic";
        String tableName = "kafka_tvf_session_table";
        KafkaUtils.recrate(topic);
        new Thread(() -> {
            int max = Integer.MAX_VALUE;
            int count = 0;
            int sleepTime = 1000;
            String[] actions = {"CLICK", "VIEW", "LEAVE"};
            while (count < max) {
                if (RandomUtils.nextInt(0, 100) > 70) {
                    Long userId = RandomUtils.nextLong(15, 15);
                    String action = actions[RandomUtils.nextInt(2, 3)];
                    String eventTime = DateUtils.toIsoString();
                    String message = String.format("%s,%s,%s", userId, action, eventTime);
                    KafkaUtils.sendMessage(topic, message, true);
                    count++;
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(sleepTime);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        // 运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.executeSql(String.format("""
                CREATE TABLE %s (
                `userId` BIGINT,
                `action` STRING,
                `eventTime` STRING,
                et AS TO_TIMESTAMP_LTZ(eventTime, 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
                WATERMARK FOR et AS et - INTERVAL '2' SECOND
                ) WITH (
                'connector' = 'kafka',
                'topic' = '%s',
                'properties.bootstrap.servers' = 'mj01:6667',
                'properties.group.id' = 'yjxxtliyi',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'csv'
                )
                """, tableName, topic));
//        tableEnvironment.from(tableName).execute().print();
        tableEnvironment.sqlQuery(String.format("""
                        SELECT window_start, window_end, userId, action, COUNT(*) as event_count, NOW() pt
                        FROM SESSION(TABLE %s PARTITION BY (userId, action), DESCRIPTOR(et), INTERVAL '2' SECONDS)
                        GROUP BY userId, action, window_start, window_end;
                        """, tableName))
                .execute()
                .print();
    }

}
