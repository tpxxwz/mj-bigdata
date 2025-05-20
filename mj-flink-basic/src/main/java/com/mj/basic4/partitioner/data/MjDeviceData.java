package com.mj.basic4.partitioner.data;

import lombok.*;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
@Data
@NoArgsConstructor  // Flink 必须的
@AllArgsConstructor
public class MjDeviceData {
    private String deviceId;
    private String deviceName;
}
