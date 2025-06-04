package com.mj.bean;

import com.mj.utils.TimeConverter;
import lombok.Data;

@Data
public class TransactionBean {
    public String txId;
    public String userId;
    public double amount;
    public String status; // "success" 或 "fail"
    public Long timestamp;
    private String ts;

    public TransactionBean() {
    }

    public TransactionBean(String txId, String userId, double amount, String status, Long timestamp) {
        this.txId = txId;
        this.userId = userId;
        this.amount = amount;
        this.status = status;
        this.timestamp = timestamp;
        this.ts = TimeConverter.convertLongToDateTime(timestamp);
    }

    public TransactionBean(String txId, String userId, double amount, String status, String ts) {
        this.txId = txId;
        this.userId = userId;
        this.amount = amount;
        this.status = status;
        this.ts = ts;
        this.timestamp = TimeConverter.convertDateTimeToLong(ts);
    }
}
