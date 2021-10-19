package bigdata.clebeg.cn.quickstart.sink;

import java.util.List;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

/**
 * 生产环境中，往往需要将结果数据写入 redis 以提高数据服务请求性能。
 * 实现通过 jedis pipeline 方式写入数据到 redis，提高数据写入性能，同时保证 exactly once。
 * 业务方可以根据这个 demo 很快修改为自己业务所用。
 *
 * 输入：<key, value> key 必须是 String, Value 也是 String，当然业务方可以按照自己问题进行修改。
 * @author clebegxie
 */
public class BatchRedisSink extends RichSinkFunction<Tuple2<String, String>> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(BatchRedisSink.class);

    /**
     * batch threshold, if batch num bigger than threshold, insert.
     */
    private int threshold = 1000;

    /**
     * batch threshold, if wait time long bigger than threshold, insert.
     */
    private long waitTime = 10000;

    private String redisHost;
    private int redisPort;
    private int redisTimeout = 2000;
    private String redisPasswd;

    public BatchRedisSink(int threshold, long waitTime, String redisHost, int redisPort, int redisTimeout,
            String redisPasswd) {
        this.threshold = threshold;
        this.waitTime = waitTime;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisTimeout = redisTimeout;
        this.redisPasswd = redisPasswd;
    }

    public BatchRedisSink(int threshold, long waitTime, String redisHost, int redisPort,
            String redisPasswd) {
        this.threshold = threshold;
        this.waitTime = waitTime;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisPasswd = redisPasswd;
    }

    public BatchRedisSink(int threshold, long waitTime, String redisHost, int redisPort) {
        this.threshold = threshold;
        this.waitTime = waitTime;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    public BatchRedisSink(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }


    private ListState<Tuple2<String, String>> restDataState;
    private ListStateDescriptor<Tuple2<String, String>> restDataStateDesc;

    private List<Tuple2<String, String>> cacheData;

    private JedisPool jedisPool;
    private long lastWriteTime;

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // 定期将没有写入 redis 的数据 snapshot 到状态后端
        restDataState.clear();
        restDataState.addAll(cacheData);
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        // 初始化状态
        restDataStateDesc = new ListStateDescriptor<>("restDataState", Types.TUPLE(Types.STRING, Types.STRING));
        restDataState = ctx.getOperatorStateStore().getListState(restDataStateDesc);
        lastWriteTime = System.currentTimeMillis();
        // 如果是故障恢复，直接恢复状态
        if (ctx.isRestored()) {
            for (Tuple2<String, String> data : restDataState.get()) {
                cacheData.add(data);
            }
        }
    }

    /**
     * 遗留问题：假如一直不过来数据怎么办？
     * @param data 输入的数据
     * @param context 上下文
     * @throws Exception 抛出的异常
     */
    @Override
    public void invoke(Tuple2<String, String> data, Context context) throws Exception {
        // 这里可以做一些数据转化
        cacheData.add(data);
        long curTime = System.currentTimeMillis();

        if (cacheData.size() >= threshold || (curTime - lastWriteTime) >= waitTime) {
            // 达到阈值 批量输出
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                if (null != redisPasswd && redisPasswd.isEmpty()) {
                    jedis.auth(redisPasswd);
                }
                Pipeline pipeline = jedis.pipelined();
                for (Tuple2<String, String> curD : cacheData) {
                    pipeline.set(curD.f0, curD.f1);
                }
                pipeline.sync();
            } catch (Exception e) {
                LOG.error(
                        "Pipeline failed sync to redis, error msg %s",
                        e.getMessage()
                );
                throw e;
            } finally {
                try {
                    if (jedis != null) {
                        jedis.close();
                    }
                } catch (Exception e) {
                    LOG.error("Failed to close jedis instance, ", e);
                }
            }
        }

        // 清空之前的数据重新开始计数
        cacheData.clear();
        lastWriteTime = System.currentTimeMillis();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 jedis pool
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);

        try {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout, redisPasswd);
        } catch (Exception e) {
            LOG.error("Redis pool init failed:", e);
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}
