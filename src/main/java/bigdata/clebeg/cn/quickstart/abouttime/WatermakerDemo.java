package bigdata.clebeg.cn.quickstart.abouttime;

import bigdata.clebeg.cn.model.Order;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 通过 watermaker 机制解决一定程度上的时间乱序问题
 * 通过 socket 方式模拟订单数据发送，转为订单对象，然后统计5s内的订单支付金额，改变最大允许乱序时间 观察窗口触发的机制。
 * input：
 * o1,10,10,1634473761000
 * o2,10,30,1634473762000
 * o4,10,20,1634473764000
 * o6,10,10,1634473766000
 * o5,10,20,1634473765000
 * o3,10,20,1634473763000
 * o10,10,10,1634473770000
 *
 * output:
 * Order(orderId=o1, userId=10, money=10, payTime=1634473761000)
 * Order(orderId=o2, userId=10, money=30, payTime=1634473762000)
 * Order(orderId=o4, userId=10, money=20, payTime=1634473764000)
 * Order(orderId=o6, userId=10, money=10, payTime=1634473766000)
 * result> (10,WindowInfo=[20211017202920,20211017202925),Order(orderId=o1, userId=10, money=10, payTime=1634473761000):Order(orderId=o2, userId=10, money=30, payTime=1634473762000):Order(orderId=o4, userId=10, money=20, payTime=1634473764000):)
 * Order(orderId=o5, userId=10, money=20, payTime=1634473765000)
 * Order(orderId=o3, userId=10, money=20, payTime=1634473763000)
 * side_output> Order(orderId=o3, userId=10, money=20, payTime=1634473763000)
 * Order(orderId=o10, userId=10, money=10, payTime=1634473770000)
 * result> (10,WindowInfo=[20211017202925,20211017202930),Order(orderId=o6, userId=10, money=10, payTime=1634473766000):Order(orderId=o5, userId=10, money=20, payTime=1634473765000):)
 * @author clebeg
 * @create 2021-10-17 09:23
 **/
public class WatermakerDemo {
    private static String formatPattern = "yyyyMMddHHmmss";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("bigdata1", 9999);

        SingleOutputStreamOperator<Order> orders = source.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String input) throws Exception {
                try {
                    String[] cols = input.split(",");
                    String orderId = cols[0];
                    int userId = Integer.parseInt(cols[1]);
                    int money = Integer.parseInt(cols[2]);
                    long payTime = Long.parseLong(cols[3]);
                    return new Order(orderId, userId, money, payTime);
                } catch (Exception e) {
                    System.out.println("Error input data = " + input);
                }
                return null;
            }
        }).filter(new FilterFunction<Order>() {
            @Override
            public boolean filter(Order order) throws Exception {
                // true will output
                return order != null;
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((order, timestamp) -> order.getPayTime()));

        orders.print();

        OutputTag<Order> outputTag = new OutputTag<>("side_output", TypeInformation.of(Order.class));
        SingleOutputStreamOperator<Tuple2<Integer, String>> res = orders.keyBy(order -> order.getUserId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(0))
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<Order, Tuple2<Integer, String>, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer key,
                                      TimeWindow window,
                                      Iterable<Order> orders,
                                      Collector<Tuple2<Integer, String>> collector) throws Exception {

                        String info = String.format("WindowInfo=[%s,%s),",
                                DateFormatUtils.format(window.getStart(), formatPattern),
                                DateFormatUtils.format(window.getEnd(), formatPattern));
                        StringBuilder sb = new StringBuilder();
                        sb.append(info);
                        for (Order order : orders) {
                            sb.append(order + ":");
                        }
                        collector.collect(Tuple2.of(key, sb.toString()));
                    }
                });
        DataStream<Order> sideOutput = res.getSideOutput(outputTag);
        sideOutput.print("side_output");
        res.print("result");
        env.execute();
    }
}
