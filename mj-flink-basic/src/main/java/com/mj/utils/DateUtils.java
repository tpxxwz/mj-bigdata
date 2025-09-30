package com.mj.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateUtils {
    // ISO 8601 带毫秒格式
    private static final DateTimeFormatter ISO_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public static String toIsoString() {
        return toIsoString(LocalDateTime.now());
    }

    /**
     * LocalDateTime -> ISO 8601 字符串
     */
    public static String toIsoString(LocalDateTime ldt) {
        if (ldt == null) return null;
        return ldt.format(ISO_FORMATTER);
    }

    /**
     * 可选：解析 ISO 8601 字符串 -> LocalDateTime
     */
    public static LocalDateTime fromIsoString(String isoString) {
        if (isoString == null || isoString.isEmpty()) return null;
        return LocalDateTime.parse(isoString, ISO_FORMATTER);
    }
}
