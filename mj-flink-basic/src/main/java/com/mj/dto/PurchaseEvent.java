package com.mj.dto;

import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class PurchaseEvent {
    public String userId;
    public Long purchaseTime;  // 事件时间戳（毫秒）
    public Double amount;
}
