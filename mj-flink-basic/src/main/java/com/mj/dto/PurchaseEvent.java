package com.mj.dto;

import lombok.*;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
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
