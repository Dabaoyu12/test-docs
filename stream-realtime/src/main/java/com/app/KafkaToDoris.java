package com.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaToDoris {

    private static final String KAFKA_BROKERS = "cdh01:9092";
    private static final String[] TOPICS = {
            "ods_page_log",
            "ods_action_log",
            "ods_display_log",
            "ods_error_log",
            "ods_start_log"
    };

    private static final String DORIS_JDBC_URL = "jdbc:mysql://cdh01:9030/aaa";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASS = "";

    public static void main(String[] args) throws Exception {
        // Kafka consumer 配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "doris_loader_" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPICS));

        // Doris JDBC
        Connection conn = DriverManager.getConnection(DORIS_JDBC_URL, DORIS_USER, DORIS_PASS);
        conn.setAutoCommit(false);
        String insertSql = "INSERT INTO dwd_logs_unified " +
                "(log_date, log_type, mid_id, user_id, ts, event_time, topic, partition_id, `offset`, " +
                "session_id, page_id, item_id, action_id, display_id, error_code, os, app_version, ch, ar) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement pstmt = conn.prepareStatement(insertSql);

        System.out.println("=== Doris Loader Started ===");

        int batchSize = 0;
        final int BATCH_LIMIT = 100;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (records.isEmpty()) {
                continue;
            }

            for (ConsumerRecord<String, String> record : records) {
                try {
                    JSONObject root = JSON.parseObject(record.value());

                    JSONObject common = root.getJSONObject("common");
                    JSONObject page = root.getJSONObject("page");
                    JSONObject err = root.getJSONObject("err");

                    String logType = getLogType(record.topic());
                    String mid = common != null ? common.getString("mid") : null;
                    String uid = common != null ? common.getString("uid") : null;
                    Long ts = root.getLong("ts");

                    if (mid == null || ts == null) {
                        System.out.println("脏数据过滤: " + record.value());
                        continue;
                    }

                    Timestamp eventTime = new Timestamp(ts);

                    pstmt.setDate(1, new java.sql.Date(ts));
                    pstmt.setString(2, logType);
                    pstmt.setString(3, mid);
                    pstmt.setString(4, uid);
                    pstmt.setLong(5, ts);
                    pstmt.setTimestamp(6, eventTime);
                    pstmt.setString(7, record.topic());
                    pstmt.setInt(8, record.partition());
                    pstmt.setLong(9, record.offset());

                    pstmt.setString(10, common != null ? common.getString("sid") : null);
                    pstmt.setString(11, page != null ? page.getString("page_id") : null);
                    pstmt.setString(12, page != null ? page.getString("item") : null);
                    pstmt.setString(13, null); // action_id 暂不展开 actions 数组
                    pstmt.setString(14, null); // display_id 暂不展开 displays 数组
                    pstmt.setString(15, err != null ? err.getString("error_code") : null);

                    pstmt.setString(16, common != null ? common.getString("os") : null);
                    pstmt.setString(17, common != null ? common.getString("vc") : null);
                    pstmt.setString(18, common != null ? common.getString("ch") : null);
                    pstmt.setString(19, common != null ? common.getString("ar") : null);

                    pstmt.addBatch();
                    batchSize++;

                    if (batchSize >= BATCH_LIMIT) {
                        pstmt.executeBatch();
                        conn.commit();
                        pstmt.clearBatch();
                        batchSize = 0;
                        System.out.println("已批量写入 " + BATCH_LIMIT + " 条到 Doris");
                    }

                } catch (Exception e) {
                    System.err.println("处理失败: " + record.value());
                    e.printStackTrace();
                }
            }
        }
    }

    private static String getLogType(String topic) {
        switch (topic) {
            case "ods_page_log": return "page";
            case "ods_action_log": return "action";
            case "ods_display_log": return "display";
            case "ods_error_log": return "error";
            case "ods_start_log": return "start";
            default: return "unknown";
        }
    }
}
