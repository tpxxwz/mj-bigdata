package com.mj.basic4.union.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc  统一环境报告
 */
@Data
@NoArgsConstructor  // Flink 必须的
@AllArgsConstructor
public class MjEnvReport {
    private String deviceId;
    private String type;       // "TEMPERATURE" 或 "HUMIDITY"
    private double value;
    private long timestamp;
}
