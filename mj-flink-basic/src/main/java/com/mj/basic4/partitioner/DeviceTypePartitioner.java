package com.mj.basic4.partitioner;

import org.apache.flink.api.common.functions.Partitioner;

public class DeviceTypePartitioner implements Partitioner<String> {

    @Override
    public int partition(String deviceId, int numPartitions) {
        // 工业设备强制分区0
        if (deviceId.startsWith("IND")) {
            return 0;
        }else if (deviceId.startsWith("MED")) {// 医疗设备强制分区1
            return 1;
        }else {// 其他设备轮询分配
            return 2;
        }
    }
}
