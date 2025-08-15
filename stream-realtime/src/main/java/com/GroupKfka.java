package com;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * IDEA 直接运行的单文件程序：
 * - main(args): args[0]=bootstrapServers (默认 cdh01:9092)
 *               args[1]=inputTopic (默认 realtime_log)
 *               --localTest 可作为第三个参数，表明先发送内置示例消息到 inputTopic 用于调试
 *
 * 使用方式（IDEA Run Configuration -> Program arguments）:
 *   cdh01:9092 realtime_log --localTest
 *
 * 注意：确保你的 pom 里包含 kafka 客户端（你的 pom 已含 flink-connector-kafka，通常会带 kafka-clients）。
 */
public class GroupKfka {

    public static void main(String[] args) throws Exception {
        String bootstrap = args.length > 0 ? args[0] : "cdh01:9092";
        String inputTopic = args.length > 1 ? args[1] : "realtime_log";
        boolean localTest = false;
        for (String a : args) if ("--localTest".equalsIgnoreCase(a)) localTest = true;

        System.out.printf("Start LogRouterApp with bootstrap=%s inputTopic=%s localTest=%s%n", bootstrap, inputTopic, localTest);

        // 如果需要先写入示例消息以便本地测试
        if (localTest) {
            produceSampleMessages(bootstrap, inputTopic);
            // 等一小会儿让消息到 broker
            Thread.sleep(1500);
        }

        LogRouter router = new LogRouter(bootstrap, inputTopic);
        Runtime.getRuntime().addShutdownHook(new Thread(router::shutdown));
        router.run();
    }

    /**
     * 向 inputTopic 产生几条示例 JSON（使用你提供的数据）
     */
    private static void produceSampleMessages(String bootstrap, String inputTopic) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "1");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);

        List<String> samples = Arrays.asList(
                // 示例1
                "{\"actions\":[{\"action_id\":\"cart_add\",\"item\":\"16\",\"item_type\":\"sku_id\",\"ts\":1755184132158}],\"common\":{\"ar\":\"14\",\"ba\":\"xiaomi\",\"ch\":\"xiaomi\",\"is_new\":\"0\",\"md\":\"xiaomi 13 Pro \",\"mid\":\"mid_460\",\"os\":\"Android 13.0\",\"sid\":\"6547b5da-2b09-4279-8a42-49319b89790e\",\"uid\":\"491\",\"vc\":\"v2.1.134\"},\"displays\":[{\"item\":\"21\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":0},{\"item\":\"18\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":1}],\"page\":{\"during_time\":9252,\"from_pos_id\":5,\"from_pos_seq\":15,\"item\":\"16\",\"item_type\":\"sku_id\",\"last_page_id\":\"cart\",\"page_id\":\"good_detail\"},\"ts\":1755184129158}",
                // 示例2
                "{\"actions\":[{\"action_id\":\"cart_add\",\"item\":\"21\",\"item_type\":\"sku_id\",\"ts\":1755183055170}],\"common\":{\"ar\":\"25\",\"ba\":\"realme\",\"ch\":\"xiaomi\",\"is_new\":\"1\",\"md\":\"realme Neo2\",\"mid\":\"mid_283\",\"os\":\"Android 13.0\",\"sid\":\"763a1891-d64e-41c1-8457-ce1c8a1e39a2\",\"uid\":\"493\",\"vc\":\"v2.1.134\"},\"displays\":[{\"item\":\"34\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":0}],\"page\":{\"during_time\":6838,\"from_pos_id\":4,\"from_pos_seq\":2,\"item\":\"21\",\"item_type\":\"sku_id\",\"last_page_id\":\"good_detail\",\"page_id\":\"good_detail\"},\"ts\":1755183052170}",
                // 示例3
                "{\"common\":{\"ar\":\"24\",\"ba\":\"iPhone\",\"ch\":\"Appstore\",\"is_new\":\"1\",\"md\":\"iPhone 13\",\"mid\":\"mid_246\",\"os\":\"iOS 13.3.1\",\"sid\":\"47e14454-c9ce-44cd-ab31-236fa5661196\",\"uid\":\"364\",\"vc\":\"v2.1.111\"},\"page\":{\"during_time\":10332,\"item\":\"1498\",\"item_type\":\"order_id\",\"last_page_id\":\"order\",\"page_id\":\"payment\"},\"ts\":1755184052043}",
                // 示例4
                "{\"common\":{\"ar\":\"27\",\"ba\":\"iPhone\",\"ch\":\"Appstore\",\"is_new\":\"0\",\"md\":\"iPhone 14\",\"mid\":\"mid_353\",\"os\":\"iOS 13.2.9\",\"sid\":\"ce8c29f3-3c2b-4a7c-948f-66504411af2a\",\"uid\":\"417\",\"vc\":\"v2.1.132\"},\"page\":{\"during_time\":14574,\"item\":\"1487\",\"item_type\":\"order_id\",\"last_page_id\":\"order\",\"page_id\":\"payment\"},\"ts\":1755183634286}"
        );

        try {
            for (int i = 0; i < samples.size(); i++) {
                ProducerRecord<String, String> rec = new ProducerRecord<>(inputTopic, samples.get(i));
                producer.send(rec, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Sample message send failed: " + exception.getMessage());
                    } else {
                        System.out.printf("Sample message sent to %s partition=%d offset=%d%n", metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });
            }
            producer.flush();
        } finally {
            producer.close();
        }
    }

    /**
     * 负责从 inputTopic 消费并分流到 dim_* topic 的核心类
     */
    static class LogRouter {
        private final String bootstrap;
        private final String inputTopic;
        private final KafkaConsumer<String, String> consumer;
        private final KafkaProducer<String, String> producer;
        private final AtomicBoolean running = new AtomicBoolean(true);

        private static final String TOPIC_ACTION = "dim_action";
        private static final String TOPIC_DISPLAY = "dim_display";
        private static final String TOPIC_PAGE = "dim_page";
        private static final String TOPIC_START = "dim_start";
        private static final String TOPIC_ERR = "dim_err";

        LogRouter(String bootstrap, String inputTopic) {
            this.bootstrap = bootstrap;
            this.inputTopic = inputTopic;

            Properties cprops = new Properties();
            cprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            cprops.put(ConsumerConfig.GROUP_ID_CONFIG, "log-router-group-idea");
            cprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            cprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            this.consumer = new KafkaConsumer<>(cprops);
            consumer.subscribe(Collections.singletonList(inputTopic));

            Properties pprops = new Properties();
            pprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            pprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            pprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            pprops.put(ProducerConfig.ACKS_CONFIG, "1");
            pprops.put(ProducerConfig.RETRIES_CONFIG, 3);
            pprops.put(ProducerConfig.LINGER_MS_CONFIG, 5);

            this.producer = new KafkaProducer<>(pprops);
        }

        public void run() {
            System.out.println("LogRouter running. Polling from topic: " + inputTopic);
            try {
                while (running.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    if (records.isEmpty()) continue;

                    for (ConsumerRecord<String, String> rec : records) {
                        String msg = rec.value();
                        if (msg == null || msg.trim().isEmpty()) {
                            sendToTopic(TOPIC_ERR, null, buildErrPayload("empty_message", msg).toJSONString());
                            continue;
                        }

                        try {
                            JSONObject root = JSON.parseObject(msg);
                            boolean routed = false;

                            // start
                            if (root.containsKey("start")) {
                                JSONObject out = new JSONObject();
                                out.put("start", root.get("start"));
                                out.put("common", root.get("common"));
                                out.put("ts", root.get("ts"));
                                sendToTopic(TOPIC_START, getMid(root), out.toJSONString());
                                routed = true;
                            }

                            // actions
                            if (root.containsKey("actions")) {
                                JSONArray actions = root.getJSONArray("actions");
                                if (actions != null) {
                                    for (int i = 0; i < actions.size(); i++) {
                                        JSONObject action = actions.getJSONObject(i);
                                        JSONObject out = new JSONObject();
                                        out.put("action", action);
                                        out.put("common", root.get("common"));
                                        out.put("ts", action.containsKey("ts") ? action.get("ts") : root.get("ts"));
                                        sendToTopic(TOPIC_ACTION, getMid(root), out.toJSONString());
                                    }
                                    routed = true;
                                }
                            }

                            // displays
                            if (root.containsKey("displays")) {
                                JSONArray displays = root.getJSONArray("displays");
                                if (displays != null) {
                                    for (int i = 0; i < displays.size(); i++) {
                                        JSONObject display = displays.getJSONObject(i);
                                        JSONObject out = new JSONObject();
                                        out.put("display", display);
                                        out.put("common", root.get("common"));
                                        out.put("ts", root.get("ts"));
                                        sendToTopic(TOPIC_DISPLAY, getMid(root), out.toJSONString());
                                    }
                                    routed = true;
                                }
                            }

                            // page
                            if (root.containsKey("page")) {
                                JSONObject out = new JSONObject();
                                out.put("page", root.get("page"));
                                out.put("common", root.get("common"));
                                out.put("ts", root.get("ts"));
                                sendToTopic(TOPIC_PAGE, getMid(root), out.toJSONString());
                                routed = true;
                            }

                            if (!routed) {
                                sendToTopic(TOPIC_ERR, getMid(root), buildErrPayload("no_route_matched", msg).toJSONString());
                            }

                        } catch (Exception ex) {
                            JSONObject err = buildErrPayload("parse_or_process_error", msg);
                            err.put("error", ex.getMessage());
                            sendToTopic(TOPIC_ERR, null, err.toJSONString());
                            System.err.println("Error processing message: " + ex.getMessage());
                        }
                    }

                    try {
                        consumer.commitSync();
                    } catch (Exception e) {
                        System.err.println("commitSync failed: " + e.getMessage());
                    }
                }
            } finally {
                shutdown();
            }
        }

        private String getMid(JSONObject root) {
            if (root == null) return null;
            try {
                JSONObject common = root.getJSONObject("common");
                if (common != null) return common.getString("mid");
            } catch (Exception ignored) {}
            return null;
        }

        private JSONObject buildErrPayload(String reason, String original) {
            JSONObject obj = new JSONObject();
            obj.put("reason", reason);
            obj.put("original", original);
            obj.put("ts", System.currentTimeMillis());
            return obj;
        }

        private void sendToTopic(String topic, String key, String value) {
            ProducerRecord<String, String> r = (key != null) ? new ProducerRecord<>(topic, key, value) : new ProducerRecord<>(topic, value);
            producer.send(r, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Failed to send to " + topic + ": " + exception.getMessage());
                }
            });
        }

        public void shutdown() {
            if (!running.getAndSet(false)) return;
            System.out.println("Shutting down LogRouter...");
            try { consumer.wakeup(); } catch (Exception ignored) {}
            try { producer.flush(); producer.close(); } catch (Exception ignored) {}
            try { consumer.close(); } catch (Exception ignored) {}
        }
    }
}
