package com.mj.dto;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public  class Order {
    public String userId;    // 用户ID
    public Double amount;    // 订单金额
    public String orderId;   // 订单编号

    public Order() {}

    public Order(String userId, Double amount, String orderId) {
        this.userId = userId;
        this.amount = amount;
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    @Override
    public String toString() {
        return "Order{" +
                "userId='" + userId + '\'' +
                ", amount=" + amount +
                ", orderId='" + orderId + '\'' +
                '}';
    }
}