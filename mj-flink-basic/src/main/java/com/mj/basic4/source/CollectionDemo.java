package com.mj.basic4.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Arrays;
import java.util.List;

public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 准备数据集合
        List<String> data = Arrays.asList(
                "aa", "bb", "cc", "dd",
                "ee", "ff", "gg", "hh"
        );
        // 3. 从集合创建数据流
        DataStreamSource<String> source = env.fromCollection(data);
        // 4. 打印数据流
        source.print();
        // 5. 执行作业
        env.execute("Collection DataStream Demo");
    }
}
