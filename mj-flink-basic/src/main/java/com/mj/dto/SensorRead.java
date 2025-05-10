package com.mj.dto;

public class SensorRead {
    private String device;
    private Float temperature;

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public Float getTemperature() {
        return temperature;
    }

    public void setTemperature(Float temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorRead{" +
                "device=" + device +
                ", temperature='" + temperature + '\'' +
                '}';
    }
}
