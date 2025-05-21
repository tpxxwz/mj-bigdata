package com.mj.basic5.data;

import com.mj.utils.TimeConverter;

public class WaterMarkData {
    private String userId;
    private Long eventTime;
    private String ts;
    private Integer money;

    public WaterMarkData() {
    }
    public WaterMarkData(String userId, Long eventTime, Integer money) {
        this.userId = userId;
        this.eventTime = eventTime;
        this.ts = TimeConverter.convertLongToDateTime(eventTime);
        this.money = money;
    }
    public WaterMarkData(String userId, String ts, Integer money) {
        this.userId = userId;
        this.eventTime = TimeConverter.convertDateTimeToLong(ts);
        this.ts = ts;
        this.money = money;
    }

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
        return "WaterMarkData{" +
                "userId='" + userId + '\'' +
                ", eventTime=" + eventTime +
                ", ts='" + ts + '\'' +
                ", money=" + money +
                '}';
    }
}
