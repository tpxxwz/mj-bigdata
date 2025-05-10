package com.mj.dto;

public class SensorReading {
    private String deviceId;
    private long timestamp;
    private double temperature;

    // 构造函数、getters、setters
    public SensorReading() {}

    public SensorReading(String deviceId, long timestamp, double temperature) {
        this.deviceId = deviceId;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "deviceId='" + deviceId + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }

}
