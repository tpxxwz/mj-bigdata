package com.mj.bean;


import lombok.Data;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
@Data
public  class OrderInfo {
    public Integer amount;    // 订单金额
    public String orderId;   // 订单编号
    public Long eventTime;   // 订单编号

    public OrderInfo() {}

    public OrderInfo( String orderId,Integer amount,Long eventTime) {
        this.amount = amount;
        this.orderId = orderId;
        this.eventTime = eventTime;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "OrderInfo{" +
                "orderId='" + orderId + '\'' +
                ", amount=" + amount +
                ", eventTime='" + eventTime + '\'' +
                '}';
    }
}