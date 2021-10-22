package bigdata.clebeg.cn.quickstart.state;

import bigdata.clebeg.cn.quickstart.state.FlinkUvDemo.AppMidInfo;
import bigdata.clebeg.cn.quickstart.state.FlinkUvDemo.AppVisitEvent;
import cn.hutool.core.date.DateUtil;
import java.util.Date;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class UVProcessFunction extends KeyedProcessFunction<Tuple2<String, Long>, AppVisitEvent, AppMidInfo> {
    // map state 保持出现过的 uid
    private MapState<Long, Boolean> uidState;

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
    public void processElement(AppVisitEvent elem,
            KeyedProcessFunction<Tuple2<String, Long>, AppVisitEvent, AppMidInfo>.Context ctx,
            Collector<AppMidInfo> collector) throws Exception {
        // 当前的水印
        long visitTime = elem.getVisitTime();
        String visitDay = DateUtil.format(new Date(visitTime), "yyyyMMdd");

        MapStateDescriptor<Long, Boolean> uidStateDesc = new MapStateDescriptor<>(visitDay + "_uidState", TypeInformation.of(Long.class),
                TypeInformation.of(Boolean.class));

        uidStateDesc.enableTimeToLive(ttlConfig);

        uidState = getRuntimeContext().getMapState(uidStateDesc);

        if (elem.getVisitTime() + 1 <= ctx.timerService().currentWatermark()) {
            // 数据延迟到达：可以侧流输出，稍后实现
            System.out.println(String.format("late data:" + elem));
            return;
        }

        if (!uidState.contains(elem.getUid())) {
            // 如果用户没有出行过，则更新数据: uv+1
            uidState.put(elem.getUid(), true);
            collector.collect(new AppMidInfo(elem.getAppid(), elem.getUid(), elem.getVisitTime(), elem.getPartId(), 1));
        } else {
            collector.collect(new AppMidInfo(elem.getAppid(), elem.getUid(), elem.getVisitTime(), elem.getPartId(), 0));
        }
    }
}