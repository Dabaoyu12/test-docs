package dby.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;

public class DwsPvUvJob {
    public static void main(String[] args) throws Exception {
        // 1. Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Kafka 配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "cdh01:9092");
        props.setProperty("group.id", "dws_pv_uv_group");

        // 3. Kafka Source (DWD 层的数据)
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>("dwd_user_05", new SimpleStringSchema(), props);
        consumer.setStartFromEarliest();
        DataStream<String> source = env.addSource(consumer);

        // 转 JSON
        DataStream<JSONObject> jsonStream = source.map(JSON::parseObject);

        // 4. 简单聚合：统计 PV/UV
        DataStream<String> result = jsonStream
                .map(x -> {
                    JSONObject obj = new JSONObject();
                    obj.put("user_id", x.getString("user_id"));
                    obj.put("item_id", x.getString("item_id"));
                    obj.put("shop_id", x.getString("shop_id"));
                    obj.put("event_type", x.getString("event_type"));
                    return obj.toJSONString();
                });

        // 5. Kafka Sink topic 自动创建
        String sinkTopic = "dws_pv_uv_05";
        createKafkaTopicIfNotExists(props, sinkTopic);

        result.addSink(new FlinkKafkaProducer<>(sinkTopic, new SimpleStringSchema(), props));

        result.print();

        // 6. 执行
        env.execute("DWS PV/UV Aggregation Job");
    }

    // ================= Kafka Topic 自动创建工具方法 =================
    private static void createKafkaTopicIfNotExists(Properties props, String topicName) {
        try (AdminClient adminClient = AdminClient.create(props)) {
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
                adminClient.createTopics(Collections.singleton(newTopic));
                System.out.println("自动创建 Kafka Topic: " + topicName);
            } else {
                System.out.println("Kafka Topic 已存在: " + topicName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
