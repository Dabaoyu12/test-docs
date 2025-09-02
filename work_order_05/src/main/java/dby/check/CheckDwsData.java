package dby.check;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;

public class CheckDwsData {

    public static void main(String[] args) {
        String bootstrapServers = "cdh01:9092";

        // 要消费的 topics
        List<String> topics = Arrays.asList(
                "dws_user_visit_summary",
                "dws_cart_summary",
                "dws_order_summary"
        );

        for (String topic : topics) {
            System.out.println("\n===== 读取 " + topic + " 前 10 条数据 =====");

            Properties props = new Properties();
            props.put("bootstrap.servers", bootstrapServers);
            props.put("group.id", "check-dws-data-" + topic); // 每个 topic 用不同 group 避免 offset 冲突
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("auto.offset.reset", "earliest"); // 从头读

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(topic));

                int count = 0;
                while (count < 10) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("offset=%d, key=%s, value=%s%n",
                                record.offset(), record.key(), record.value());
                        count++;
                        if (count >= 10) break;
                    }
                }
            }
        }
    }
}
