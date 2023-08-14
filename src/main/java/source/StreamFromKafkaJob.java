package source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


public class StreamFromKafkaJob {

    private final KafkaSource<String> source;
    private final SinkFunction<Tuple2<String, Integer>> sink;


    public StreamFromKafkaJob(KafkaSource<String> source, SinkFunction<Tuple2<String, Integer>> sink) {
        this.source = source;
        this.sink = sink;
    }

    public static void main(String[] args) throws Exception {

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("127.0.0.1:9094")
                .setTopics("my_output_topic")
                .setGroupId("my_group_topi2c")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();


        StreamFromKafkaJob job = new StreamFromKafkaJob(kafkaSource, new MyPrintSink());

        job.execute();
    }

    /**
     * 任务执行
     *
     * @throws Exception
     */
    private void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> data = env.fromSource(this.source, WatermarkStrategy.noWatermarks(), "myKafkaSource");


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = data.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
            String[] split = value.split(" ");
            out.collect(Tuple2.of(split[0], 1));
        }).returns(Types.TUPLE(Types.STRING,Types.INT));

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordAndOne.keyBy((KeySelector<Tuple2<String, Integer>, Object>) value -> value.f0).sum(1);


        result.addSink(sink);
        env.execute();
    }
}
