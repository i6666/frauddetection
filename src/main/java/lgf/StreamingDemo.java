package lgf;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhuang.ma
 * @date 2022/5/6
 */
public class StreamingDemo {
    public static void main(String[] args) throws Exception {
//        streamTest1();
        streamTest2();


    }

    private static void streamTest2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple3<Integer, Integer, Integer>> data = new ArrayList<>();
        data.add(new Tuple3<>(0,1,0));
        data.add(new Tuple3<>(0,1,1));
        data.add(new Tuple3<>(0,2,2));
        data.add(new Tuple3<>(0,1,3));
        data.add(new Tuple3<>(1,2,5));
        data.add(new Tuple3<>(1,2,9));
        data.add(new Tuple3<>(1,2,11));
        data.add(new Tuple3<>(1,2,13));

        DataStreamSource<Tuple3<Integer, Integer, Integer>> items = env.fromCollection(data);
        items.keyBy(0).max(2).printToErr();
        //打印结果
        String jobName = "user defined streaming source";
//        6> (0,1,0)
//        6> (0,1,1)
//        6> (0,2,2)
//        6> (0,1,3)
//        6> (1,2,5)
//        6> (1,2,9)
//        6> (1,2,11)
//        6> (1,2,13)
        env.execute(jobName);
    }

    private static void streamTest1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置数据来源 并行度为1
        DataStreamSource<Item> text = env.addSource(new MyStreamingSource()).setParallelism(1);
        DataStream<Item> item = text.map(value -> value);

        item.print().setParallelism(1);

        String jobName = "user defined streaming source";
        env.execute(jobName);
    }
}
