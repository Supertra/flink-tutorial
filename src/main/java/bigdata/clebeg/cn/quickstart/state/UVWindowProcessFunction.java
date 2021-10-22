package bigdata.clebeg.cn.quickstart.state;

import bigdata.clebeg.cn.quickstart.state.FlinkUvDemo.AppMidInfo;
import bigdata.clebeg.cn.quickstart.state.FlinkUvDemo.AppStatInfo;
import cn.hutool.core.date.DateUtil;
import java.util.Date;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UVWindowProcessFunction extends ProcessWindowFunction<AppMidInfo, AppStatInfo, String, TimeWindow> {
    // value state 保留出现过的用户数
    private ValueState<Long> uvState;

    private StateTtlConfig ttlConfig;

    @Override
    public void open(Configuration parameters) throws Exception {
        // init state
        ttlConfig = StateTtlConfig
                .newBuilder(Time.days(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                .cleanupInRocksdbCompactFilter(1000L)
                .build();
    }
    @Override
    public void process(String appid,
            ProcessWindowFunction<AppMidInfo, AppStatInfo, String, TimeWindow>.Context ctx,
            Iterable<AppMidInfo> items, Collector<AppStatInfo> collector) throws Exception {
        // 当前的水印
        long startTime = ctx.window().getStart();
        String windowDay = DateUtil.format(new Date(startTime), "yyyyMMdd");
        ValueStateDescriptor<Long> uvStateDesc = new ValueStateDescriptor<>(windowDay + "_uvState", TypeInformation.of(Long.class));
        uvStateDesc.enableTimeToLive(ttlConfig);
        uvState = getRuntimeContext().getState(uvStateDesc);

        if (uvState.value() == null) {
            uvState.update(0L);
        }
        long uv = uvState.value();
        for (AppMidInfo item : items) {
            uv += item.getPartUV();
        }
        uvState.update(uv);
        collector.collect(new AppStatInfo(appid, windowDay, startTime, uv));
    }
}
