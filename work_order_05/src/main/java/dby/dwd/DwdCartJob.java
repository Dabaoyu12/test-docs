package dby.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 从 dwd_order_item_05 中抽取数据 -> 转换为 cart 事件 -> 输出到 dwd_cart_05
 */
public class DwdCartJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "cdh01:9092");
        props.setProperty("group.id", "dwd_cart_job");

        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>("dwd_order_item_05", new SimpleStringSchema(), props);
        consumer.setStartFromEarliest();
        DataStream<String> source = env.addSource(consumer);

        // 转换为 cart 事件
        DataStream<String> cartStream = source
                .map(JSON::parseObject)
                .map(json -> {
                    JSONObject cart = new JSONObject();
                    cart.put("event_type", "cart");
                    cart.put("user_id", json.getString("user_id"));
                    cart.put("item_id", json.getString("item_id"));
                    cart.put("sku_id", json.getString("sku_id"));
                    cart.put("shop_id", json.getString("shop_id"));
                    cart.put("ts", json.getString("ts"));
                    // 生成 1~10 的随机加购数量
                    int qty = ThreadLocalRandom.current().nextInt(1, 11);
                    cart.put("quantity", qty);
                    return cart.toJSONString();
                });

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "dwd_cart_05",
                (KafkaSerializationSchema<String>) (element, timestamp) ->
                        new ProducerRecord<>("dwd_cart_05", element.getBytes()),
                props,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        cartStream.addSink(producer);
        cartStream.print();

        env.execute("Build dwd_cart_05 from dwd_order_item_05 with random quantity");
    }
}
