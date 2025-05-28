package com.mj.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
@Data
@NoArgsConstructor  // Flink 必须的
@AllArgsConstructor
@ToString
public class Alert {
    public String message;
    public long timestamp;
}
