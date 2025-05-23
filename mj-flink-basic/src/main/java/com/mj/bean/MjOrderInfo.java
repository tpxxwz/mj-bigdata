package com.mj.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
@Data
@NoArgsConstructor  // Flink 必须的
@AllArgsConstructor
public class MjOrderInfo {
    private String orderId;
    private String userId;
    private Integer amount;
    private Long ts;  // 事件时间戳（毫秒）
}
