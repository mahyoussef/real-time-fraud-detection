package com.flinklearn.realtime.factory;


import com.flinklearn.realtime.common.Utils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaBuilder {
    public static FlinkKafkaConsumer<String> createFlinkKafkaConsumer(String topicName) {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
        String consumerGroupId = Utils.CONSUMER_GROUP_ID;
        consumerProperties.setProperty("group.id", consumerGroupId);
        FlinkKafkaConsumer<String> myConsumer =
                new FlinkKafkaConsumer<>(topicName,
                        new SimpleStringSchema(), consumerProperties);
        myConsumer.setStartFromLatest();
        return myConsumer;
    }

    public static FlinkKafkaProducer<String> createFlinkKafkaProducer(String topicName) {
        Properties producer_properties = new Properties();
        producer_properties.setProperty("bootstrap.servers", "localhost:9092");
        producer_properties.setProperty("acks","1");
        return new FlinkKafkaProducer<String>(topicName,new SimpleStringSchema(),producer_properties);
    }
}
