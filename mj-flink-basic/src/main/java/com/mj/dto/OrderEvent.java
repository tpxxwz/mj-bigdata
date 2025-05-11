package com.mj.dto;

import lombok.*;

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
