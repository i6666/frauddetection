package source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MyPrintSink extends PrintSinkFunction<Tuple2<String, Integer>> {

}
