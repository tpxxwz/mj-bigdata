package com.mj.dto;

import com.mj.utils.TimeConverter;

public class UserWindow {
    private String userId;
    private Long eventTime;
    private String ts;
    private Integer money;

    public UserWindow() {
    }

    public UserWindow(String userId,Integer money,Long eventTime) {
        this.userId = userId;
        this.eventTime = eventTime;
        this.money = money;
        this.ts = TimeConverter.convertLongToDateTime(eventTime);
    }

    // Getter 和 Setter 方法
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public Integer getMoney() {
        return money;
    }

    public void setMoney(Integer money) {
        this.money = money;
    }

    @Override
    public String toString() {
        return "UserWindow{" +
                "userId='" + userId + '\'' +
                ", eventTime=" + eventTime +
                ", ts='" + ts + '\'' +
                ", money=" + money +
                '}';
    }
}
