package com.mj.bean;

import com.mj.utils.TimeConverter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
@Data
@NoArgsConstructor
public class WaterMarkData {
    private String userId;
    private Long eventTime;
    private String ts;
    private Integer money;
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
}
