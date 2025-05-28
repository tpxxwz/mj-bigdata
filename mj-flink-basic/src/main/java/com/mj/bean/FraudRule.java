package com.mj.bean;

import lombok.Data;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 定义欺诈规则类
 */
@Data
public class FraudRule {
    private String ruleId;
    private double maxAmount;
    private int maxLocationFrequency;
    public FraudRule() {}  // 空构造函数

    public FraudRule(String ruleId, double maxAmount) {
        this.ruleId = ruleId;
        this.maxAmount = maxAmount;
    }
}
