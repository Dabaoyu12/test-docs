package com.app1;

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
        String kafkaTopic = "realtime_log";
        String hbaseZk = args.length > 1 ? args[1] : "cdh01";

        System.out.printf("Start HBaseKafkaIngestApp with kafka=%s topic=%s hbaseZk=%s%n",
                kafkaBootstrap, kafkaTopic, hbaseZk);

        // ---- HBase init ----
        Configuration hConf = HBaseConfiguration.create();
        hConf.set("hbase.zookeeper.quorum", hbaseZk);
        hConf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection hbaseConn = ConnectionFactory.createConnection(hConf);
        Admin admin = hbaseConn.getAdmin();

        String family = "cf";
        Map<String, BufferedMutator> mutators = new HashMap<>();
        List<String> tables = Arrays.asList(
                "dim_action_log",
                "dim_display_log",
                "dim_page_log",
                "dim_start_log",
                "dim_error_log"
        );

        try {
            // 检查并创建表
            for (String tableNameStr : tables) {
                TableName tableName = TableName.valueOf(tableNameStr);
                if (!admin.tableExists(tableName)) {
                    System.out.println("Creating table: " + tableNameStr);
                    ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder.of(family);
                    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
                            .setColumnFamily(cfDesc)
                            .build();
                    admin.createTable(tableDesc);
                }
                BufferedMutator mutator = hbaseConn.getBufferedMutator(TableName.valueOf(tableNameStr));
                mutators.put(tableNameStr, mutator);
            }

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

                    System.out.println("Polled " + records.count() + " records from Kafka.");

                    Map<String, List<Put>> batchPutsByTable = new HashMap<>();

                    for (ConsumerRecord<String, String> rec : records) {
                        String msg = rec.value();
                        if (msg == null || msg.trim().isEmpty()) continue;

                        JSONObject root;
                        try {
                            root = JSON.parseObject(msg);
                        } catch (Exception ex) {
                            System.err.println("JSON parse error, skip record. msg=" + msg);
                            continue;
                        }

                        String mid = extractMid(root);
                        Long ts = extractTs(root);

                        if (mid == null || mid.trim().isEmpty()) {
                            System.err.println("Skip record: missing mid. msg=" + msg);
                            continue;
                        }
                        if (ts == null || ts <= 0L) {
                            System.err.println("Skip record: invalid ts. msg=" + msg);
                            continue;
                        }

                        boolean routed = false;

                        // actions
                        if (root.containsKey("actions")) {
                            JSONArray actions = root.getJSONArray("actions");
                            if (actions != null) {
                                for (int i = 0; i < actions.size(); i++) {
                                    JSONObject act = actions.getJSONObject(i);
                                    if (act == null) continue;
                                    String sub = act.getString("action_id");
                                    String rowkey = buildRowKey(mid, ts);
                                    Put put = new Put(Bytes.toBytes(rowkey));
                                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("event_type"), Bytes.toBytes("action"));
                                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("mid"), Bytes.toBytes(mid));
                                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ts"), Bytes.toBytes(ts));
                                    if (sub != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes("sub"), Bytes.toBytes(sub));
                                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("payload"), Bytes.toBytes(act.toJSONString()));
                                    batchPutsByTable.computeIfAbsent("dim_action_log", k -> new ArrayList<>()).add(put);
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
                                    if (disp == null) continue;
                                    String sub = disp.getString("item");
                                    String rowkey = buildRowKey(mid, ts);
                                    Put put = new Put(Bytes.toBytes(rowkey));
                                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("event_type"), Bytes.toBytes("display"));
                                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("mid"), Bytes.toBytes(mid));
                                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ts"), Bytes.toBytes(ts));
                                    if (sub != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes("sub"), Bytes.toBytes(sub));
                                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("payload"), Bytes.toBytes(disp.toJSONString()));
                                    batchPutsByTable.computeIfAbsent("dim_display_log", k -> new ArrayList<>()).add(put);
                                }
                                routed = true;
                            }
                        }

                        // page
                        if (root.containsKey("page")) {
                            Object page = root.get("page");
                            if (page != null) {
                                String rowkey = buildRowKey(mid, ts);
                                Put put = new Put(Bytes.toBytes(rowkey));
                                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("event_type"), Bytes.toBytes("page"));
                                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("mid"), Bytes.toBytes(mid));
                                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ts"), Bytes.toBytes(ts));
                                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("payload"), Bytes.toBytes(JSON.toJSONString(page)));
                                batchPutsByTable.computeIfAbsent("dim_page_log", k -> new ArrayList<>()).add(put);
                                routed = true;
                            }
                        }

                        // start
                        if (root.containsKey("start")) {
                            Object start = root.get("start");
                            if (start != null) {
                                String rowkey = buildRowKey(mid, ts);
                                Put put = new Put(Bytes.toBytes(rowkey));
                                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("event_type"), Bytes.toBytes("start"));
                                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("mid"), Bytes.toBytes(mid));
                                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("ts"), Bytes.toBytes(ts));
                                put.addColumn(Bytes.toBytes(family), Bytes.toBytes("payload"), Bytes.toBytes(JSON.toJSONString(start)));
                                batchPutsByTable.computeIfAbsent("dim_start_log", k -> new ArrayList<>()).add(put);
                                routed = true;
                            }
                        }

                        // 没有路由成功
                        if (!routed) {
                            System.err.println("Skip record: no valid event found. msg=" + msg);
                        }
                    } // end for records

                    // 批量写入 HBase
                    Map<String, Integer> batchCount = new HashMap<>();
                    boolean allHbaseOk = true;
                    for (Map.Entry<String, List<Put>> e : batchPutsByTable.entrySet()) {
                        List<Put> puts = e.getValue();
                        if (puts == null || puts.isEmpty()) continue;
                        try {
                            mutators.get(e.getKey()).mutate(puts);
                            mutators.get(e.getKey()).flush();
                            batchCount.put(e.getKey(), puts.size());
                        } catch (Exception he) {
                            System.err.println("HBase put failed for table " + e.getKey() + ": " + he.getMessage());
                            allHbaseOk = false;
                        }
                    }
                    System.out.println("Batch summary: " + batchCount);

                    if (allHbaseOk) {
                        consumer.commitSync();
                    }
                } // end while
            } catch (Exception ex) {
                System.err.println("Kafka consumer interrupted: " + ex.getMessage());
            } finally {
                consumer.close();
                mutators.values().forEach(m -> {
                    try { m.close(); } catch (Exception ignored) {}
                });
                hbaseConn.close();
            }
        } finally {
            admin.close();
        }
    }

    private static String buildRowKey(String mid, long ts) {
        return mid + "_" + ts + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    private static String extractMid(JSONObject obj) {
        if (obj.containsKey("mid")) return obj.getString("mid");
        if (obj.containsKey("common")) {
            JSONObject common = obj.getJSONObject("common");
            if (common != null && common.containsKey("mid")) return common.getString("mid");
        }
        return null;
    }

    private static Long extractTs(JSONObject obj) {
        if (obj.containsKey("ts")) return obj.getLong("ts");
        return System.currentTimeMillis();
    }
}
