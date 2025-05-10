package com.mj.basic4.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileDemo {
    public static void main(String[] args) throws Exception {
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 创建文件源
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("D:/tar_source/hello.txt")
                )
                .build();
        // 3. 从文件源创建数据流
        DataStreamSource<String> fileStream = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "file-source"
        );
        // 4. 打印数据流
        fileStream.print();
        // 5. 执行作业
        env.execute("File Stream Processing Demo");
    }
}
