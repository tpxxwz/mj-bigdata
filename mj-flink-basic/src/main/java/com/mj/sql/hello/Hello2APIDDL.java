package com.mj.sql.hello;

import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class Hello2APIDDL {
    public static void main(String[] args) {
        // 运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 创建表：方案1
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("mj-flink-basic/tmp/dept.txt"))
                .build();
        DataStreamSource<String> deptSource = environment.fromSource(
                source,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "file-source");
        Table deptTable = tableEnvironment.fromDataStream(deptSource);
        deptTable.execute().print();
    }
}
