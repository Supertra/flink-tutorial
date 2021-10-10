package bigdata.clebeg.cn.quickstart.connectors;

import bigdata.clebeg.cn.model.Order;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;

/**
 * jdbc sink demo
 *
 * @author clebeg
 * @create 2021-10-10 17:38
 **/
public class JDBCSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Order> orderDS = env.addSource(new RandomSourceFunction());
        SinkFunction<Order> jdbcSink = JdbcSink.sink(
                "insert into orders (order_id, user_id, money, pay_time) values (?, ?, ?, ?)",
                (statement, order) -> {
                    statement.setString(1, order.getOrderId());
                    statement.setInt(2, order.getUserId());
                    statement.setInt(3, order.getMoney());
                    statement.setLong(4, order.getPayTime());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://bigdata1:3306/flink_demo")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );
        orderDS.addSink(jdbcSink);

        env.execute();
    }


    private static class RandomSourceFunction extends RichParallelSourceFunction<Order> {
        private volatile boolean flag = true;

        @Override
        public void run(SourceContext ctx) throws Exception {
            while (flag) {
                Order order = new Order();
                order.setOrderId(UUID.randomUUID().toString());
                order.setUserId(RandomUtils.nextInt(1, 5));
                order.setMoney(RandomUtils.nextInt(1, 101));
                order.setPayTime(System.currentTimeMillis());
                ctx.collect(order);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
