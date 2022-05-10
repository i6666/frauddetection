package datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test1 {
    public static void main(String[] args) throws Exception {
        //Flink 原生的序列化器可以高效的操作tuples 和 pojos
//        tuplesTest();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Person> elements = env.fromElements(new Person("tom-m", 24),
                new Person("tom-o", 45),
                new Person("tom-f", 26),
                new Person("tom", 2));

        SingleOutputStreamOperator<Person> adults = elements.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) throws Exception {
                return person.age >= 18;
            }
        });
        adults.print();
        env.execute();


    }


    private static void tuplesTest() {
        Tuple2<String, Integer> person = Tuple2.of("strong", 20);
        String name = person.f0;
        Integer age = person.f1;
    }
}
