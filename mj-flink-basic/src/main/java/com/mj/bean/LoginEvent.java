package com.mj.bean;

import com.mj.utils.TimeConverter;
import lombok.Data;

@Data
public class LoginEvent {
    private String userId;
    private String status; // "success" 或 "fail"
    private Long timestamp;
    private String ts;

    public LoginEvent() {
    }
    public LoginEvent(String userId, String status, Long timestamp) {
        this.userId = userId;
        this.status = status;
        this.timestamp = timestamp;
        this.ts = TimeConverter.convertLongToDateTime(timestamp);

    }

    public LoginEvent(String userId, String status, String ts) {
        this.userId = userId;
        this.status = status;
        this.ts = ts;
        this.timestamp = TimeConverter.convertDateTimeToLong(ts);

    }
}
