package bigdata.clebeg.cn.quickstart.connectors;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * 统计单词数量并且输出到redis
 * flink run -t yarn-per-job -c bigdata.clebeg.cn.quickstart.connectors.WordCntSinkRedisDemo -yjm 512 -ytm 512 --detached flink-tutorial-1.0-SNAPSHOT.jar
 * @author clebeg
 * @create 2021-10-10 20:49
 **/
public class WordCntSinkRedisDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // nc -lk 9999
        DataStreamSource<String> lines = env.socketTextStream("bigdata1", 9999);

        SingleOutputStreamOperator<Tuple2<String, Long>> res = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String input, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] splits = input.split("\\s");
                for (String word : splits) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        }).keyBy(wn -> wn.f0).sum(1);


        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("bigdata1")
                .setPort(6379)
                .setTimeout(5000)
                .build();
        RedisSink<Tuple2<String, Long>> redisSink = new RedisSink<>(flinkJedisPoolConfig, new RedisMapper<Tuple2<String, Long>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "wordcnt_sink_redis_demo");
            }

            @Override
            public String getKeyFromData(Tuple2<String, Long> wn) {
                return wn.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, Long> wn) {
                return wn.f1.toString();
            }
        });
        res.print();
        res.addSink(redisSink);

        env.execute();
    }
}
