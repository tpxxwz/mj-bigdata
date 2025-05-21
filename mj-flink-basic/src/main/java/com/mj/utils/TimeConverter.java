package com.mj.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

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
    public static Long convertDateTimeToLong(String datetime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime = LocalDateTime.parse(datetime, formatter);
        long timestamp = dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        System.out.println(timestamp); // 输出：1750381800000
        return timestamp;
    }

    public static void main(String[] args) {
        String timeStr = "2025-05-20 14:30:00";
        TimeConverter.convertDateTimeToLong(timeStr);
    }

}
