package com.mj.dto;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
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
