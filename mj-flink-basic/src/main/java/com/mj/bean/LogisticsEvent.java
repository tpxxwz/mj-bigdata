package com.mj.bean;

import com.mj.utils.TimeConverter;
import lombok.Data;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
@Data
public class LogisticsEvent {
    public String orderId;
    public Long shippingTime;
    public String status;
    public String ts;
    public LogisticsEvent() {}
    public LogisticsEvent(String orderId, String ts, String status) {
        this.orderId = orderId;
        this.shippingTime = TimeConverter.convertDateTimeToLong(ts);
        this.status = status;
    }
}
