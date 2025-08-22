package com.app;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SimpleKafkaConsumer {

    private static final String KAFKA_BROKERS = "cdh01:9092";
    private static final String[] TOPICS = {
            "ods_page_log",
            "ods_action_log",
            "ods_display_log",
            "ods_error_log",
            "ods_start_log"
    };

    public static void main(String[] args) {
        // 配置 consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);

        // 每次运行都用新的 group.id，避免 offset 卡住
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_consumer_" + System.currentTimeMillis());

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPICS));

        System.out.println("=== Kafka Consumer Started ===");
        System.out.println("订阅的topic: " + Arrays.toString(TOPICS));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                System.out.println("本次poll到的消息数: " + records.count());

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s%n",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}


