package bigdata.clebeg.cn.quickstart.state;

import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * flink 实时统计用户数的案例
 * 参考资料：
 * 1. https://www.jianshu.com/p/090686bb98bd
 * 2. https://www.cnblogs.com/Springmoon-venn/p/10919648.html
 * 需求：通过 flink 实时统计没有 app 的独立用户数
 * 1. 可能某些 app 的用户数特别大
 * 2. 可能状态会特别大
 * input:
 * appid,uid,visit_time
 *
 * output:
 * uvRes> (app1,1634400000000,1)
 * uvRes> (app1,1634400000000,2)
 * uvRes> (app1,1634400000000,3)
 * uvRes> (app1,1634486400000,1)
 * @author clebegxie
 */
public class FlinkUvDemo {
    private static final int FILED_NUM = 3;

    public static void main(String[] args) throws Exception {
        // step1. init env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1000));
        // step2. init source
//        DataStream<String> visitDS = getDataStream(env);
        DataStreamSource<String> visitDS = env.socketTextStream("bigdata1", 9999);
        // step3. transformation
        SingleOutputStreamOperator<AppVisitEvent> events = visitDS.map(new MapFunction<String, AppVisitEvent>() {
            @Override
            public AppVisitEvent map(String input) throws Exception {
                String[] split = input.split(",");
                if (split.length == FILED_NUM) {
                    try {
                        String appid = split[0];
                        long uid = Long.parseLong(split[1]);
                        long visitTime = Long.parseLong(split[2]);
                        return new AppVisitEvent(appid, uid, visitTime, uid % 1000);
                    } catch (Exception e) {
                        System.out.printf("Bad row:%s, err msg=%s\n", input, e.getMessage());
                    }
                }
                return null;
            }
        }).filter(event -> event != null);
        events.print("events");
        // 3.1 告诉 flink 事件时间
        SingleOutputStreamOperator<AppVisitEvent> eventTimeDS = events.assignTimestampsAndWatermarks(
                WatermarkStrategy.<AppVisitEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((event, timestamp) -> event.getVisitTime())
        );

        // 3.1 按照 partId 分组，parId 是通过 uid 取模
        KeyedStream<AppVisitEvent, Tuple2<String, Long>> keyStep1 = eventTimeDS
                .keyBy(new KeySelector<AppVisitEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> getKey(AppVisitEvent visitEvent) throws Exception {
                        return Tuple2.of(visitEvent.appid, visitEvent.partId);
                    }
                });
        // 3.3 经过第一步窗口处理
        SingleOutputStreamOperator<AppMidInfo> keyStep1Res = keyStep1.process(new UVProcessFunction());
        keyStep1Res.print("keyStep1Res");

        SingleOutputStreamOperator<AppStatInfo> uvRes = keyStep1Res.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AppMidInfo>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((event, timestamp) -> event.getVisitTime())
                ).keyBy(elem -> elem.appid)
                .window(TumblingEventTimeWindows.of(Time.seconds(1L)))
                .process(new UVWindowProcessFunction());
        // step4. 输出结果
        uvRes.print("uvRes");

        // step5. execute
        env.execute("FlinkUvDemo");
    }

    private static DataStream<String> getDataStream(StreamExecutionEnvironment env) {
        // input format: string,uin32,13longtime
        return env.fromElements(
                "app1,1001,1634447599000",
                "app1,1001,1634451199000",
                "app1,1001,1634483599000",
                "app1,1002,1634483599000",
                "app1,1003,1634483599000",
                "app1,1001,1634486359000",
                "app1,1001,1634486400000",
                "app1,1001,1634486459000"
        );
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    @Data
    public static class AppVisitEvent {
        private String appid;
        private long uid;
        private long visitTime;
        private long partId;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    @Data
    public static class AppMidInfo {
        private String appid;
        private long uid;
        private long visitTime;
        private long partId;
        private long partUV;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    @Data
    public static class AppStatInfo {
        private String appid;
        private String dayStr;
        private long windowStart;
        private long dayUv;
    }
}
