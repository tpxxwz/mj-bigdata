package com.mj.trans;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ExecutionModeEnv {
    public static void main(String[] args) throws Exception {
        //1.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       //2.
        StreamExecutionEnvironment localEnv = StreamExecutionEnvironment.createLocalEnvironment();
        //3.
        StreamExecutionEnvironment remoteEnv = StreamExecutionEnvironment
                .createRemoteEnvironment("mj01",1234,"path/to/jarFile.jar");
        //设置执行模式、批、流、自动选择
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //设置并行度
        env.setParallelism(1);

        env.execute();




    }
}
