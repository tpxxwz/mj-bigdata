package com.mj.basic5.watermark;

import com.mj.bean.WaterMarkData;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class LargeTransactionWatermarkGenerator implements WatermarkGenerator<WaterMarkData> {
    @Override
    public void onEvent(WaterMarkData event, long eventTimestamp, WatermarkOutput output) {
        // 检测到大额交易时立即生成水位线
        if (event.getMoney()>10000) {
            output.emitWatermark(new Watermark(event.getEventTime()));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 无需周期性生成
    }
}