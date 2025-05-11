package com.mj.dto;


import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ClickEvent {
    public String userId;
    public Long clickTime;  // 事件时间戳（毫秒）
    public String adId;
}
