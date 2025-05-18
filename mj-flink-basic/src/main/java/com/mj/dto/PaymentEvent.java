package com.mj.dto;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class PaymentEvent {
    private String orderId;
    private Long payTime; // 事件时间戳（毫秒）
    public PaymentEvent(){

    }
    public PaymentEvent(String orderId, Long payTime) {
        this.orderId = orderId;
        this.payTime = payTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Long getPayTime() {
        return payTime;
    }

    public void setPayTime(Long payTime) {
        this.payTime = payTime;
    }
}
