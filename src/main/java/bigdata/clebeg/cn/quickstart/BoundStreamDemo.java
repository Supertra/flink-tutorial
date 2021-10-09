package bigdata.clebeg.cn.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

public class BoundStreamDemo {
    public static void main(String[] args) throws Exception {
        // init env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> input = env.readTextFile("datasets/input_dir");

        AggregateOperator<Tuple2<String, Integer>> sum = input.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>)
                (s, collector) -> {
            String[] words = s.split("\\s");
            for (String word : words) {
                if (!word.trim().isEmpty()) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        }).groupBy(0).sum(1);
        sum.print();

//        sum.writeAsText("datasets/output_dir/boundstreamdemo.txt");
        env.execute(BoundStreamDemo.class.getSimpleName());
    }
}
