package dby.dws;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class DwsAsyncKafkaJob {

    public static void main(String[] args) throws Exception {
        // 1. Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // Kafka 基础配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "cdh01:9092");
        props.setProperty("group.id", "dws-async");

        // =======================
        // PV / UV 来源: dwd_page_log_05
        // =======================
        FlinkKafkaConsumer<String> pageConsumer = new FlinkKafkaConsumer<>(
                "dwd_page_log_05",
                new SimpleStringSchema(),
                props
        );
        pageConsumer.setStartFromEarliest();
        DataStreamSource<String> pageLog = env.addSource(pageConsumer);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> uvpv = pageLog
                .map(line -> {
                    String userId = extractField(line, "user_id");
                    return new Tuple3<>("dws_user_visit_summary", userId, 1L);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>(){}));

        // UV（去重用户数）
        SingleOutputStreamOperator<Tuple3<String, String, Long>> uv = uvpv
                .keyBy(x -> x.f0 + "_" + x.f1) // 按 topic+user_id 去重
                .reduce((a, b) -> a)
                .map(v -> new Tuple3<>("dws_user_visit_summary", "uv", 1L))
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>(){}));

        // PV（所有记录数）
        SingleOutputStreamOperator<Tuple3<String, String, Long>> pv = uvpv
                .map(v -> new Tuple3<>("dws_user_visit_summary", "pv", 1L))
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>(){}));

        // =======================
        // 加购件数 来源: dwd_cart_05
        // =======================
        FlinkKafkaConsumer<String> cartConsumer = new FlinkKafkaConsumer<>(
                "dwd_cart_05",
                new SimpleStringSchema(),
                props
        );
        cartConsumer.setStartFromEarliest();
        DataStreamSource<String> cartLog = env.addSource(cartConsumer);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> cartCnt = cartLog
                .map(line -> {
                    String qtyStr = extractField(line, "quantity");
                    long qty = 0L;
                    try {
                        qty = Long.parseLong(qtyStr.trim());
                    } catch (Exception ignored) {}
                    return new Tuple3<>("dws_cart_summary", "cart_cnt", qty);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>(){}))
                .filter(x -> x.f2 > 0); // 过滤掉数量 <= 0 的加购事件

        // =======================
        // 支付金额 来源: dwd_payment_05
        // =======================
        FlinkKafkaConsumer<String> payConsumer = new FlinkKafkaConsumer<>(
                "dwd_payment_05",
                new SimpleStringSchema(),
                props
        );
        payConsumer.setStartFromEarliest();
        DataStreamSource<String> payLog = env.addSource(payConsumer);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> payAmount = payLog
                .map(line -> {
                    String payStr = extractField(line, "pay_amount");
                    long pay = 0L;
                    try {
                        // 元转分，避免精度丢失
                        pay = new BigDecimal(payStr.trim())
                                .movePointRight(2)
                                .longValue();
                    } catch (Exception ignored) {}
                    return new Tuple3<>("dws_order_summary", "pay_amount", pay);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>(){}));

        // =======================
        // 退款金额 来源: dwd_refund_05
        // =======================
        FlinkKafkaConsumer<String> refundConsumer = new FlinkKafkaConsumer<>(
                "dwd_refund_05",
                new SimpleStringSchema(),
                props
        );
        refundConsumer.setStartFromEarliest();
        DataStreamSource<String> refundLog = env.addSource(refundConsumer);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> refundAmount = refundLog
                .map(line -> {
                    String refundStr = extractField(line, "refund_amount");
                    long refund = 0L;
                    try {
                        refund = new BigDecimal(refundStr.trim())
                                .movePointRight(2)
                                .longValue();
                    } catch (Exception ignored) {}
                    return new Tuple3<>("dws_order_summary", "refund_amount", refund);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>(){}));

        // =======================
        // 合并所有指标流
        // =======================
        DataStream<Tuple3<String, String, Long>> unionStream =
                uv.union(pv)
                        .union(cartCnt)
                        .union(payAmount)
                        .union(refundAmount);

        unionStream.print();

        // =======================
        // 使用 AsyncFunction 写 Kafka
        // =======================
        AsyncDataStream.unorderedWait(
                unionStream,
                new AsyncKafkaSink(),
                30, TimeUnit.SECONDS,
                100
        );

        env.execute("DWS Async Kafka Job");
    }

    // =======================================
    // 简易 JSON 解析工具 (避免依赖外部库)
    // =======================================
    private static String extractField(String json, String field) {
        try {
            String key = "\"" + field + "\":";
            int idx = json.indexOf(key);
            if (idx >= 0) {
                int start = idx + key.length();
                char c = json.charAt(start);
                if (c == '"') { // string 类型
                    int end = json.indexOf('"', start + 1);
                    return json.substring(start + 1, end);
                } else { // 数字类型
                    int end = start;
                    while (end < json.length() && "0123456789.-".indexOf(json.charAt(end)) >= 0) {
                        end++;
                    }
                    return json.substring(start, end);
                }
            }
        } catch (Exception ignored) {}
        return "";
    }

    // =======================================
    // 自定义 Async Kafka Sink
    // =======================================
    public static class AsyncKafkaSink extends RichAsyncFunction<Tuple3<String, String, Long>, Void> {
        private transient KafkaProducer<String, String> producer;
        private transient AdminClient adminClient;
        private transient Set<String> topicCache;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Properties props = new Properties();
            props.put("bootstrap.servers", "cdh01:9092");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<>(props);
            adminClient = AdminClient.create(props);
            topicCache = new HashSet<>();
        }

        private synchronized void createTopicIfNotExists(String topic) {
            try {
                if (!topicCache.contains(topic)) {
                    Set<String> existingTopics = adminClient.listTopics().names().get();
                    if (!existingTopics.contains(topic)) {
                        NewTopic newTopic = new NewTopic(topic, 3, (short) 1);
                        adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                        System.out.println("创建 Kafka Topic: " + topic);
                    }
                    topicCache.add(topic);
                }
            } catch (ExecutionException e) {
                if (e.getCause() != null && e.getCause().getMessage().contains("TopicExistsException")) {
                    topicCache.add(topic);
                } else {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void asyncInvoke(Tuple3<String, String, Long> input, ResultFuture<Void> resultFuture) {
            CompletableFuture.runAsync(() -> {
                try {
                    String topic = input.f0;
                    String key = input.f1;
                    String value = String.valueOf(input.f2);

                    createTopicIfNotExists(topic);
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                    producer.send(record);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).thenAccept((ignored) -> resultFuture.complete(Collections.emptyList()));
        }

        @Override
        public void close() throws Exception {
            if (producer != null) producer.close();
            if (adminClient != null) adminClient.close();
        }
    }
}
