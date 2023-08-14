package source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class MyKafkaSource{
            //生产kafka消息
            Properties props = new Properties();
            KafkaSource<String> build = KafkaSource.<String>builder().setTopics("my_input_topic")
                    .setGroupId("my_group_topic")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SimpleStringSchema()).build();

}
