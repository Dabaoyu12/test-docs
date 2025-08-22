package com.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.Properties;

public class KafkaToDorisDwdOrder {

    private static final String DORIS_JDBC_URL = "jdbc:mysql://cdh01:9030/aaa";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASS = "";

    public static void main(String[] args) throws Exception {
        // Doris JDBC
        Connection dConn = DriverManager.getConnection(DORIS_JDBC_URL, DORIS_USER, DORIS_PASS);
        dConn.setAutoCommit(false);

        String insertSql = "INSERT INTO dwd_ecommerce_order " +
                "(id, activity_name, activity_type, activity_desc, start_time, end_time, create_time) " +
                "VALUES (?,?,?,?,?,?,?)";
        PreparedStatement pstmt = dConn.prepareStatement(insertSql);

        // Kafka 配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "cdh01:9092");
        // 动态 groupId，避免 offset 被卡住
        props.put("group.id", "doris-dwd-sync-group-" + System.currentTimeMillis());
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("ods_ecommerce_order"));

        int batchSize = 0;
        final int BATCH_LIMIT = 100;
        int totalWritten = 0;

        System.out.println("开始消费 Kafka 主题 ods_ecommerce_order → Doris.dwd_ecommerce_order ...");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofSeconds(5));
            if (records.isEmpty()) {
                System.out.println("当前 poll 没有数据 ...");
                continue;
            }

            for (ConsumerRecord<String, String> record : records) {
                // 打印原始 Kafka 消息，方便排查
                System.out.println("消费到消息: " + record.value());

                JSONObject root = JSONObject.parseObject(record.value());
                JSONObject obj = root.getJSONObject("after");
                if (obj == null) {
                    System.out.println("跳过空 after 消息: " + record.value());
                    continue;
                }

                pstmt.setLong(1, obj.getLongValue("id"));
                pstmt.setString(2, obj.getString("activity_name"));
                pstmt.setString(3, obj.getString("activity_type"));
                pstmt.setString(4, obj.getString("activity_desc"));

                pstmt.setTimestamp(5, obj.getLong("start_time") != null
                        ? new java.sql.Timestamp(obj.getLong("start_time")) : null);
                pstmt.setTimestamp(6, obj.getLong("end_time") != null
                        ? new java.sql.Timestamp(obj.getLong("end_time")) : null);
                pstmt.setTimestamp(7, obj.getLong("create_time") != null
                        ? new java.sql.Timestamp(obj.getLong("create_time")) : null);

                pstmt.addBatch();
                batchSize++;

                if (batchSize >= BATCH_LIMIT) {
                    int[] counts = pstmt.executeBatch();
                    dConn.commit();
                    totalWritten += sum(counts);
                    pstmt.clearBatch();
                    batchSize = 0;
                    System.out.println("已写入 DWD 订单表，总计: " + totalWritten);
                }
            }

            if (batchSize > 0) {
                int[] counts = pstmt.executeBatch();
                dConn.commit();
                totalWritten += sum(counts);
                pstmt.clearBatch();
                batchSize = 0;
                System.out.println("已写入 DWD 订单表，总计: " + totalWritten);
            }
        }
    }

    private static int sum(int[] counts) {
        int s = 0;
        for (int c : counts) s += c;
        return s;
    }
}
