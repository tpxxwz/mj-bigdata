package com.mj.basic4.union.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
@Data
@NoArgsConstructor  // Flink 必须的
@AllArgsConstructor
public class MjStockUpdateEvent {
    private String productId;
    private int stock;
    private long updateTime;
}
