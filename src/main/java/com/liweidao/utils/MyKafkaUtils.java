package com.liweidao.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Properties;

public class MyKafkaUtils {
    private static String kafka_server = "192.168.31.145:9092";
    private static String DEFAULT_TOPIC = "dwd_default_topic";
    private static Properties properties = new Properties();

    static {
        properties.setProperty("bootstrap.servers", kafka_server);
    }

    //数据source
    public static FlinkKafkaConsumer<String> FlinkSource(String topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    //数据sink
    public static FlinkKafkaProducer<String> FlinkSink(String topic) {
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

    //DWD的事实数据写入kafka的sink,通过<T>通过泛型编程，可以自定义KafkaSerializationSchema类型，
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 5 * 60 * 1000 + "");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, kafkaSerializationSchema, properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    //拼接Kafka相关属性到FlinkSQL的DDL
    public static String getKafkaDDL(String topic, String groupid) {
        String ddl = "'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + kafka_server + "', " +
                " 'properties.group.id' = '" + groupid + "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'  ";
        return ddl;
    }
}
