package com.mj.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class TimeConverter {
    public static String convertLongToDateTime(Long timestamp) {
        // 1. 转换为Instant对象
        Instant instant = Instant.ofEpochMilli(timestamp);
        // 2. 转为指定时区的日期时间（示例使用上海时区）
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.of("Asia/Shanghai"));
        // 3. 定义格式化器
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        // 4. 格式化为字符串
        return dateTime.format(formatter);
    }

}
