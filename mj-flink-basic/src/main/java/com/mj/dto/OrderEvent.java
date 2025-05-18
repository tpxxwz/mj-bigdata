package com.mj.dto;

import lombok.*;


/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
// 订单事件（JSON 格式示例：{"orderId":"o1","userId":"u1","amount":100,"ts":1672502400000}）
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {
    public String orderId;
    public String userId;
    public Integer amount;
    public Long ts;  // 事件时间戳（毫秒）
}
