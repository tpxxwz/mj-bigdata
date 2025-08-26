package com.mj.sql.hello;

import com.mj.bean.Emp;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import static org.apache.flink.table.api.Expressions.$;


public class Hello04APIDQL {

    public static void main(String[] args) {
        // 运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // Pojo类型
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("mj-flink-basic/tmp/emp.txt")).build();
        DataStreamSource<String> empSource = environment.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");
        SingleOutputStreamOperator<Emp> empStream = empSource.map(line -> new ObjectMapper().readValue(line, Emp.class));
        Table empTable = tableEnvironment.fromDataStream(empStream);

        // 查询数据：TableAPI--查询20部门员工信息的 编号 姓名 薪资 部门编号
        empTable.filter($("deptno").isEqual(20))
                .select($("empno"), $("ename"), $("sal"), $("deptno"))
                .execute()
                .print();

        // 查询数据：SQL--查询20部门员工信息的 编号 姓名 薪资 部门编号
        tableEnvironment.createTemporaryView("t_emp", empTable);
        tableEnvironment.sqlQuery("select empno,ename,sal,deptno from t_emp where deptno = 20")
                .execute().print();

        // 查询数据：混合方式--查询20部门员工信息的 编号 姓名 薪资 部门编号
        // SQL查询中可以直接使用table的信息
        // SQL查询结果返回的又是一个Table
        tableEnvironment.sqlQuery("select * from " + empTable + " where deptno = 20")
                .filter($("job").isEqual("ANALYST"))
                .select($("ename"), $("job"))
                .execute()
                .print();
    }

}
