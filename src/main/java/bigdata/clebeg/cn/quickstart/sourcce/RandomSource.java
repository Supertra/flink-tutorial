package bigdata.clebeg.cn.quickstart.sourcce;

import bigdata.clebeg.cn.model.Order;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * 自定义数据源-随机生成数据
 *
 * @author clebeg
 * @create 2021-10-10 11:06
 **/
public class RandomSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println(env.getParallelism());
        DataStreamSource<Order> randomSource = env.addSource(new RandomSourceFunction());

        // 12 个分区的数据，集中到一个分区来展示
        randomSource.setParallelism(1);
        randomSource.print();
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
