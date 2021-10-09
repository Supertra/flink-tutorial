package bigdata.clebeg.cn.quickstart;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 通过 stream api 实现流批一体
 */
public class BSWordCnt {
    private static final String DEFAULT_OUTPUT = "hdfs://bigdatacluster/flink-tutorial/bswordcnt/output";
    public static void main(String[] args) throws Exception {
        ParameterTool pTool = ParameterTool.fromArgs(args);
        String outputDir = pTool.get("output", DEFAULT_OUTPUT);
        // step0: 获取执行环境 env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC); // 自动根据数据源选择执行模式

        // step1: 读取数据
        DataStreamSource<String> source = env.fromElements(
                "i love china",
                "i love you",
                "i love everyone",
                "how about you"
        );

//        DataStreamSource<String> source = env.socketTextStream("bigdata1", 9999);

        // step2: transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> collector) throws Exception {
                String[] arr = input.split("\\s+");
                for (String word : arr) {
                    collector.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> wordAndCnt) throws Exception {
                return wordAndCnt.f0;
            }
        }).sum(1);

        // step3: sink
        result.print();
        result.writeAsText(outputDir);
        // step4: execute
        env.execute();

    }
}
