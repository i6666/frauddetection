package window;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import source.MyPrintSink;
import source.StreamFromKafkaJob;

public class TumblingWindowJob {

    private final KafkaSource<String> source;
    private final SinkFunction<Tuple2<String, Integer>> sink;


    public TumblingWindowJob(KafkaSource<String> source, SinkFunction<Tuple2<String, Integer>> sink) {
        this.source = source;
        this.sink = sink;
    }
    public static void main(String[] args) {


        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("127.0.0.1:9094")
                .setTopics("my_output_topic")
                .setGroupId("my_group_topi2c")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();


        TumblingWindowJob job = new TumblingWindowJob(kafkaSource, new MyPrintSink());

        job.execute();

    }

    private void execute() {
        //source
    }
}
