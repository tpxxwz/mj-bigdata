package com.mj.bean;

import lombok.Data;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 交易类事件
 */
@Data
public class Transaction {
    private String userId;
    private double amount;
    private String location;
    private long timestamp;

    public Transaction() {}  // 空构造函数用于Flink序列化

    public Transaction(String userId, double amount, String location) {
        this.userId = userId;
        this.amount = amount;
        this.location = location;
        this.timestamp = System.currentTimeMillis();
    }
}
