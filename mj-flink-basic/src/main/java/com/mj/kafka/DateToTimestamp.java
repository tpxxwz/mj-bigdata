package com.mj.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class DateToTimestamp {
    public static void main(String[] args) {
        String dateString = "2025-05-09 17:24:18";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            // 解析字符串为 Date 对象
            Date date = sdf.parse(dateString);
            // 获取对应的毫秒时间戳
            long timestamp = date.getTime();
            System.out.println("时间戳 (ms): " + timestamp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
