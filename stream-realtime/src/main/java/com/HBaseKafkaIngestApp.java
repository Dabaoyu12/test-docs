package com;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


public class HBaseKafkaIngestApp {

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = args.length > 0 ? args[0] : "cdh01:9092";
        String kafkaTopic = args.length > 1 ? args[1] : "realtime_log";
        String hbaseZk = args.length > 2 ? args[2] : "cdh01";
        String hbaseTable = args.length > 3 ? args[3] : "dim_events";

        System.out.printf("Start HBaseKafkaIngestApp with kafka=%s topic=%s hbaseZk=%s table=%s%n",
                kafkaBootstrap, kafkaTopic, hbaseZk, hbaseTable);

        // ---- HBase init ----
        Configuration hConf = HBaseConfiguration.create();
        // 根据你的集群修改 zookeeper quorum / port
        hConf.set("hbase.zookeeper.quorum", hbaseZk);
        // 默认 zookeeper client port 2181，如果不同请改
        hConf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection hbaseConn = ConnectionFactory.createConnection(hConf);
        Admin admin = hbaseConn.getAdmin();

        TableName tableName = TableName.valueOf(hbaseTable);
        String family = "cf";
        if (!admin.tableExists(tableName)) {
            System.out.println("Table " + hbaseTable + " not exists, creating...");
            ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder.of(family);
            TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(cfDesc)
                    .build();
            admin.createTable(tableDesc);
            System.out.println("Table created: " + hbaseTable);
        } else {
            System.out.println("Table exists: " + hbaseTable);
        }

        Table hTable = hbaseConn.getTable(tableName);

        // ---- Kafka consumer init ----
        Properties cprops = new Properties();
        cprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        cprops.put(ConsumerConfig.GROUP_ID_CONFIG, "hbase-ingest-group");
        cprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cprops);
        consumer.subscribe(Collections.singletonList(kafkaTopic));

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown requested.");
            running.set(false);
            try { consumer.wakeup(); } catch (Exception ignored) {}
        }));

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records == null || records.isEmpty()) continue;

                List<Put> batchPuts = new ArrayList<>();
                for (ConsumerRecord<String, String> rec : records) {
                    String msg = rec.value();
                    if (msg == null || msg.trim().isEmpty()) {
                        // skip or write an error row
                        continue;
                    }

                    try {
                        JSONObject root = JSON.parseObject(msg);
                        // route: actions -> multiple rows; displays -> multiple rows; page/start -> one row each;
                        boolean routed = false;

                        // helper to build Put
                        // rowkey: mid_ts_uuid
                        String mid = extractMid(root);
                        Long ts = extractTs(root);

                        // actions
                        if (root.containsKey("actions")) {
                            JSONArray actions = root.getJSONArray("actions");
                            if (actions != null) {
                                for (int i = 0; i < actions.size(); i++) {
                                    JSONObject act = actions.getJSONObject(i);
                                    String sub = act.containsKey("action_id") ? act.getString("action_id") : null;
                                    String rowkey = buildRowKey(mid, ts);
                                    Put put = new Put(Bytes.toBytes(rowkey));
                                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("event_type"), Bytes.toBytes("action"));
                                    if (mid != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes("mid"), Bytes.toBytes(mid));
                                    if (ts != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ts"), Bytes.toBytes(ts.toString()));
                                    if (sub != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes("sub"), Bytes.toBytes(sub));
                                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("payload"), Bytes.toBytes(act.toJSONString()));
                                    batchPuts.add(put);
                                }
                                routed = true;
                            }
                        }

                        // displays
                        if (root.containsKey("displays")) {
                            JSONArray displays = root.getJSONArray("displays");
                            if (displays != null) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject disp = displays.getJSONObject(i);
                                    String sub = disp.containsKey("item") ? disp.getString("item") : null;
                                    String rowkey = buildRowKey(mid, ts);
                                    Put put = new Put(Bytes.toBytes(rowkey));
                                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("event_type"), Bytes.toBytes("display"));
                                    if (mid != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes("mid"), Bytes.toBytes(mid));
                                    if (ts != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ts"), Bytes.toBytes(ts.toString()));
                                    if (sub != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes("sub"), Bytes.toBytes(sub));
                                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("payload"), Bytes.toBytes(disp.toJSONString()));
                                    batchPuts.add(put);
                                }
                                routed = true;
                            }
                        }

                        // page
                        if (root.containsKey("page")) {
                            Object page = root.get("page");
                            String rowkey = buildRowKey(mid, ts);
                            Put put = new Put(Bytes.toBytes(rowkey));
                            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("event_type"), Bytes.toBytes("page"));
                            if (mid != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes("mid"), Bytes.toBytes(mid));
                            if (ts != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ts"), Bytes.toBytes(ts.toString()));
                            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("payload"), Bytes.toBytes(JSON.toJSONString(page)));
                            batchPuts.add(put);
                            routed = true;
                        }

                        // start
                        if (root.containsKey("start")) {
                            Object start = root.get("start");
                            String rowkey = buildRowKey(mid, ts);
                            Put put = new Put(Bytes.toBytes(rowkey));
                            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("event_type"), Bytes.toBytes("start"));
                            if (mid != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes("mid"), Bytes.toBytes(mid));
                            if (ts != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ts"), Bytes.toBytes(ts.toString()));
                            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("payload"), Bytes.toBytes(JSON.toJSONString(start)));
                            batchPuts.add(put);
                            routed = true;
                        }

                        // 未匹配上，写原始为 err 类型（或可选择跳过）
                        if (!routed) {
                            String rowkey = buildRowKey(mid, ts);
                            Put put = new Put(Bytes.toBytes(rowkey));
                            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("event_type"), Bytes.toBytes("err"));
                            if (mid != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes("mid"), Bytes.toBytes(mid));
                            if (ts != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ts"), Bytes.toBytes(ts.toString()));
                            put.addColumn(Bytes.toBytes(family), Bytes.toBytes("payload"), Bytes.toBytes(msg));
                            batchPuts.add(put);
                        }

                    } catch (Exception ex) {
                        // 解析异常 -> 写 err 行
                        String rowkey = buildRowKey(null, System.currentTimeMillis());
                        Put put = new Put(Bytes.toBytes(rowkey));
                        put.addColumn(Bytes.toBytes(family), Bytes.toBytes("event_type"), Bytes.toBytes("err_parse"));
                        put.addColumn(Bytes.toBytes(family), Bytes.toBytes("payload"), Bytes.toBytes(msg == null ? "" : msg));
                        put.addColumn(Bytes.toBytes(family), Bytes.toBytes("error"), Bytes.toBytes(ex.getMessage()));
                        batchPuts.add(put);
                    }
                } // for records

                // 批量写入 HBase
                if (!batchPuts.isEmpty()) {
                    try {
                        hTable.put(batchPuts);
                        // 可 flush/close 由外部管理
                    } catch (Exception e) {
                        System.err.println("HBase put failed: " + e.getMessage());
                    }
                }

                // commit kafka offsets
                try {
                    consumer.commitSync();
                } catch (Exception e) {
                    System.err.println("commitSync failed: " + e.getMessage());
                }
            } // while running
        } finally {
            try { hTable.close(); } catch (Exception ignored) {}
            try { admin.close(); } catch (Exception ignored) {}
            try { hbaseConn.close(); } catch (Exception ignored) {}
            try { consumer.close(); } catch (Exception ignored) {}
        }
    }

    private static String extractMid(JSONObject root) {
        if (root == null) return null;
        try {
            JSONObject common = root.getJSONObject("common");
            if (common != null) return common.getString("mid");
        } catch (Exception ignored) {}
        return null;
    }

    private static Long extractTs(JSONObject root) {
        if (root == null) return null;
        try {
            if (root.containsKey("ts")) {
                Object t = root.get("ts");
                if (t instanceof Number) return ((Number) t).longValue();
                return Long.parseLong(t.toString());
            }
            // fallback: maybe actions[].ts
            if (root.containsKey("actions")) {
                JSONArray actions = root.getJSONArray("actions");
                if (actions != null && !actions.isEmpty()) {
                    JSONObject a = actions.getJSONObject(0);
                    if (a != null && a.containsKey("ts")) {
                        Object t = a.get("ts");
                        if (t instanceof Number) return ((Number) t).longValue();
                        return Long.parseLong(t.toString());
                    }
                }
            }
        } catch (Exception ignored) {}
        return null;
    }

    private static String buildRowKey(String mid, Long ts) {
        StringBuilder sb = new StringBuilder();
        if (mid != null && !mid.isEmpty()) sb.append(mid);
        sb.append("_");
        if (ts != null) sb.append(ts);
        else sb.append(System.currentTimeMillis());
        sb.append("_").append(UUID.randomUUID().toString().replace("-", "").substring(0, 8));
        return sb.toString();
    }
}
