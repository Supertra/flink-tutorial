package bigdata.clebeg.cn.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 从程序启动开始，输入一个单词输出 <单词, 已经出现的次数>
 * 进一步思考？
 * 1. 如果一个单词最后的结果只想保存一个怎么办？
 * 接收器保证key可以被覆盖
 * 2. 如果发送数据的网络断开一段时间怎么办？
 * 可以设置无限重启，直到网络恢复
 * 3. 如果下游接收压力太大，怎么办？
 * 可以设置窗口，对一定窗口内的数据进行去重再输出
 * 4. 如果运行碰到异常失败重启怎么办？
 * 可以开启checkpoint，代码review保证代码健壮
 * 这些方式，后续一一实现
 */
public class StreamWordCnt {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(4);

        streamEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 10000));
        DataStreamSource<String> source =  streamEnv.socketTextStream("127.0.0.1", 7777);

        SingleOutputStreamOperator<Tuple2<String, Long>> sumData = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String lines, Collector<String> collector) throws Exception {
                String[] words = lines.split("\\s");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        }).keyBy(0).sum(1);

        sumData.print();
        streamEnv.execute("StreamWordCount");
    }
}
