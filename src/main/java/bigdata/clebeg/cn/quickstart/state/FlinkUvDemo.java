package bigdata.clebeg.cn.quickstart.state;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * flink 实时统计用户数的案例 、
 * 参考资料：
 * 1. https://www.jianshu.com/p/090686bb98bd
 * 2. https://www.cnblogs.com/Springmoon-venn/p/10919648.html
 * 需求：通过 flink 实时统计没有 app 的独立用户数
 * 1. 可能某些 app 的用户数特别大
 * 2. 可能状态会特别大
 * input:
 * appid,uid,visit_time
 * @author clebegxie
 */
public class FlinkUvDemo {
    private static final int FILED_NUM = 3;

    public static void main(String[] args) throws Exception {
        // step1. init env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setStateBackend(new HashMapStateBackend());
        env.setParallelism(1);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart( 3, 60000));
        // step2. init source
        DataStream<String> visitDS = getDataStream(env);

        SingleOutputStreamOperator<AppVisitEvent> events = visitDS.map(new MapFunction<String, AppVisitEvent>() {
            @Override
            public AppVisitEvent map(String input) throws Exception {
                String[] split = input.split(",");
                if (split.length == FILED_NUM) {
                    try {
                        String appid = split[0];
                        long uid = Long.parseLong(split[1]);
                        long visitTime = Long.parseLong(split[2]);
                        return new AppVisitEvent(appid, uid, visitTime, uid % 100);
                    } catch (Exception e) {
                        System.out.println(String.format("Bad row:%s, err msg=%s", input, e.getMessage()));
                    }
                }
                return null;
            }
        }).filter(event -> event != null);

        // 告诉 flink 后面的时间使用哪一个
        events.assignTimestampsAndWatermarks(
                WatermarkStrategy.<AppVisitEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((event, timestamp) -> event.getVisitTime())
        );

        // 按照 partId 分组，parId 是通过 uid 分组得到
        KeyedStream<AppVisitEvent, AppMidKeyInfo> keyStep1 = events.keyBy(new KeySelector<AppVisitEvent, AppMidKeyInfo>() {
            @Override
            public AppMidKeyInfo getKey(AppVisitEvent visitEvent) throws Exception {
                DateTime visitDT = DateUtil.date(visitEvent.getVisitTime());
                long beginT = DateUtil.beginOfDay(visitDT).getTime();
                long endT = DateUtil.endOfDay(visitDT).getTime();
                AppMidKeyInfo appMidKey = new AppMidKeyInfo(visitEvent.appid, visitEvent.partId, endT, beginT, 0L);
                System.out.println(String.format("input=%s, output=%s", visitEvent, appMidKey));
                return appMidKey;
            }
        });
        // 经过第一步窗口处理
        SingleOutputStreamOperator<AppMidKeyInfo> keyStep1Res = keyStep1.process(new MyKeyedProcessFunction());
        keyStep1Res.print("keyStep1Res");

        SingleOutputStreamOperator<Tuple2<String, Long>> uvRes = keyStep1Res.map(
                new MapFunction<AppMidKeyInfo, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(AppMidKeyInfo item) throws Exception {
                        return Tuple2.of(item.getAppid(), item.getPartUV());
                    }
                }).keyBy(item -> item.f0).sum(1);

        uvRes.print("uvRes");

        env.execute();
    }

    private static DataStream<String> getDataStream(StreamExecutionEnvironment env) {
        // input format: string,uin32,13longtime
        return env.fromElements(
                "app1,1001,1634447599000",
                "app1,1001,1634451199000",
                "app1,1001,1634483599000",
                "app1,1001,1634486359000",
                "app1,1001,1634486400000",
                "app1,1001,1634486459000"
        );
    }

    private static class MyKeyedProcessFunction extends KeyedProcessFunction<AppMidKeyInfo, AppVisitEvent, AppMidKeyInfo> {
        MapState<Long, Integer> uidState;
        MapStateDescriptor<Long, Integer> uidStateDesc;

        ValueState<Long> uvState;
        ValueStateDescriptor<Long> uvStateDesc;

        @Override
        public void open(Configuration parameters) throws Exception {
            uidStateDesc = new MapStateDescriptor<Long, Integer>("uidState", TypeInformation.of(Long.class),
                    TypeInformation.of(Integer.class));
            uidState = getRuntimeContext().getMapState(uidStateDesc);

            uvStateDesc = new ValueStateDescriptor<Long>("uvState", TypeInformation.of(Long.class));
            uvState = getRuntimeContext().getState(uvStateDesc);
        }

        @Override
        public void processElement(AppVisitEvent appVisitEvent,
                KeyedProcessFunction<AppMidKeyInfo, AppVisitEvent, AppMidKeyInfo>.Context ctx,
                Collector<AppMidKeyInfo> collector) throws Exception {
            // 当前的水印
            long currentWatermark = ctx.timerService().currentWatermark();
            if (ctx.getCurrentKey().windowEnd + 1 <= currentWatermark) {
                // 数据延迟到达：可以侧流输出，稍后实现
                System.out.println(String.format("late data:" + appVisitEvent));
                return;
            }

            long uid = appVisitEvent.uid;
            int uidStateValue = uidState.get(uid);
            if (uidStateValue != 1) {
                uidState.put(uid, 1);
                long cnt = uvState.value();
                uvState.update(cnt + 1);
                ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey().windowEnd + 1);
            }
        }

        @Override
        public void onTimer(long timestamp,
                KeyedProcessFunction<AppMidKeyInfo, AppVisitEvent, AppMidKeyInfo>.OnTimerContext ctx,
                Collector<AppMidKeyInfo> out) throws Exception {
            String appid = ctx.getCurrentKey().getAppid();
            long partId = ctx.getCurrentKey().getPartId();
            long beginT = ctx.getCurrentKey().getWindowBegin();
            long endT = ctx.getCurrentKey().getWindowEnd();
            AppMidKeyInfo midInfo = new AppMidKeyInfo(appid, partId, endT, beginT, uvState.value());
            System.out.println(String.format("timestamp:%l, output: %s", midInfo));
            out.collect(midInfo);
            uidState.clear();
            uvState.clear();
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    @Data
    private static class AppVisitEvent {
        private String appid;
        private long uid;
        private long visitTime;
        private long partId;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    @Data
    private static class AppMidKeyInfo {
        private String appid;
        private long partId;
        private long windowEnd;
        private long windowBegin;
        private long partUV;
    }
}
