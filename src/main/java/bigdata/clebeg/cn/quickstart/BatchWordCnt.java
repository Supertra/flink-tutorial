package bigdata.clebeg.cn.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCnt {
    public static void main(String[] args) throws Exception {
        // step0: build env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // step1: source
        DataSource<String> inputDS = env.fromElements(
                "hello how are you",
                "hello my baby",
                "thanks you"
        );
        // step2: transformation
        AggregateOperator<Tuple2<String, Integer>> finalDS = inputDS.flatMap(
                (FlatMapFunction<String, Tuple2<String, Integer>>) (sentence, collector) -> {
                    for (String word : sentence.split("\\s")) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                }).groupBy(0).sum(1);

        // step3: sink
        finalDS.print();
    }
}
