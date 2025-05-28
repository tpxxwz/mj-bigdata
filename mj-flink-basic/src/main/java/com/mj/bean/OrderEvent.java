package com.mj.bean;

import com.mj.utils.TimeConverter;
import lombok.Data;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
@Data
public class OrderEvent {
    public String orderId;
    public String userId;
    public Long orderTime;

    public OrderEvent() {}

    public OrderEvent(String orderId, String userId, String ts) {
        this.orderId = orderId;
        this.userId = userId;
        this.orderTime = TimeConverter.convertDateTimeToLong(ts);
    }
}
