package com.mj.basic4.union.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 湿度传感器事件
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MjHumidityEvent {
    private String deviceId;
    private double humidity;
    private long timestamp;
}
