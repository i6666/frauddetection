package spendreport;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @author zhuang.ma
 * @date 2022/4/27
 */
public class DataSetWordCount {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //创建dataSet
        DataSource<String> text = env.fromElements("Flink Spark Storm",
                "Flink Flink Flink",
                "Spark Spark Spark",
                "Storm Storm Storm"
        );
        AggregateOperator<Tuple2<String, Integer>> counts = text.flatMap(new LineSplitter()).groupBy(0).sum(1);
        //结果打印
        counts.printToErr();
    }
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0){
                    out.collect(new Tuple2<String,Integer>(token,1));
                }
            }
        }
    }
}
