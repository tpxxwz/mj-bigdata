package com.mj.basic5.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author 码界探索
 * 微信: 252810631
 * @desc 版权所有，请勿外传
 */
public class CustomWatermarkStrategy<T>  implements WatermarkGenerator<T> {
    private long maxTimestamp = Long.MAX_VALUE + 3 + 1L;
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        this.maxTimestamp = Math.max(this.maxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(this.maxTimestamp - 3 - 1L));
    }
}
