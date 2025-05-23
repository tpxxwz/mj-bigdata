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
public class CustomWatermarkStrategy  implements WatermarkGenerator<WaterMarkData> {
    private Long delayTime = 3000L; // 延迟时间
    private Long maxTs = -Long.MAX_VALUE + delayTime + 1L; // 观察到的最大时间戳    @Override
    public void onEvent(WaterMarkData event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(event.getEventTime(),maxTs); // 更新最大时间戳
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(this.maxTs - delayTime -1L));
    }
}
