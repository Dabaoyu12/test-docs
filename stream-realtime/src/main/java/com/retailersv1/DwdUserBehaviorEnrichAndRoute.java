package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.UUID;

/**
 * 用户行为日志 DWD 层富化 + 分流任务
 * - 从 Kafka 读取 dwd_user_behavior_log
 * - 动态关联 HBase 维度表
 * - 按行为类型分流写入 Kafka + HBase
 * - 自动创建 Kafka Topic
 * - 带完整日志调试
 */
public class DwdUserBehaviorEnrichAndRoute {

    // 输出 Kafka Topics
    private static final String TOPIC_FAVOR_ADD = "dwd_interaction_favor_add";
    private static final String TOPIC_COUPON_GET = "dwd_tool_coupon_get";
    private static final String TOPIC_COUPON_USE = "dwd_tool_coupon_use";
    private static final String TOPIC_USER_REGISTER = "dwd_user_register";

    // 输出 HBase 表
    private static final String HBASE_TABLE_FAVOR_ADD = "dwd_interaction_favor_add";
    private static final String HBASE_TABLE_COUPON_GET = "dwd_tool_coupon_get";
    private static final String HBASE_TABLE_COUPON_USE = "dwd_tool_coupon_use";
    private static final String HBASE_TABLE_USER_REGISTER = "dwd_user_register";

    // Kafka 配置
    private static final String BOOTSTRAP_SERVERS = "cdh01:9092,cdh02:9092,cdh03:9092";
    private static final int TOPIC_PARTITIONS = 6;
    private static final short REPLICATION_FACTOR = 1;

    // HBase 配置
    private static final String HBASE_ZK_QUORUM = "cdh01,cdh02,cdh03";
    private static final String HBASE_NAMESPACE = ""; // 留空表示默认命名空间

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 1. 确保 Kafka Topic 存在
        ensureKafkaTopicsExist();

        // 2. Kafka Source（从 earliest 开始，确保能读历史数据）
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setGroupId("dwd-user-behavior-router")
                .setTopics("dwd_user_behavior_log")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        // 3. 解析 JSON
        SingleOutputStreamOperator<JSONObject> dwdStream = kafkaStream
                .map(line -> {
                    try {
                        System.out.println("RAW: " + line);
                        return JSONObject.parseObject(line);
                    } catch (Exception ex) {
                        System.err.println("JSON 解析失败: " + line);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .name("parse-json");

        dwdStream.print("解析成功");

        // 4. 维度表配置流
        List<String> dimTables = Arrays.asList(
                "dim_activity_info", "dim_activity_rule", "dim_activity_sku",
                "dim_base_category1", "dim_base_category2", "dim_base_category3",
                "dim_base_dic", "dim_base_province", "dim_base_region", "dim_base_trademark",
                "dim_coupon_info", "dim_coupon_range", "dim_financial_sku_cost",
                "dim_sku_info", "dim_spu_info", "dim_user_info"
        );

        DataStream<List<String>> configStream = env.fromElements(dimTables);
        MapStateDescriptor<String, List<String>> configStateDesc = new MapStateDescriptor<>(
                "dim-config",
                TypeInformation.of(String.class),
                TypeInformation.of(new TypeHint<List<String>>() {})
        );
        BroadcastStream<List<String>> broadcastStream = configStream.broadcast(configStateDesc);

        // 5. 连接流
        BroadcastConnectedStream<JSONObject, List<String>> connected = dwdStream.connect(broadcastStream);

        // 6. 富化 + 路由
        SingleOutputStreamOperator<RoutedData> routedStream = connected
                .process(new EnrichAndRouteFunction(configStateDesc, HBASE_ZK_QUORUM))
                .name("enrich-and-route");

        routedStream.print("🔄 富化后");

        // 7. 分流
        DataStream<JSONObject> favorAddStream = routedStream
                .filter(data -> "favor_add".equals(data.route))
                .map(data -> data.json);

        DataStream<JSONObject> couponGetStream = routedStream
                .filter(data -> "coupon_get".equals(data.route))
                .map(data -> data.json);

        DataStream<JSONObject> couponUseStream = routedStream
                .filter(data -> "coupon_use".equals(data.route))
                .map(data -> data.json);

        DataStream<JSONObject> userRegisterStream = routedStream
                .filter(data -> "user_register".equals(data.route))
                .map(data -> data.json);

        // 8. 打印各路输出（调试）
        favorAddStream.print("🎯 FAVOR_ADD");
        couponGetStream.print("🎯 COUPON_GET");
        couponUseStream.print("🎯 COUPON_USE");
        userRegisterStream.print("🎯 USER_REGISTER");

        // 9. Kafka Sinks
        KafkaSink<String> favorAddKafkaSink = buildKafkaSink(TOPIC_FAVOR_ADD);
        KafkaSink<String> couponGetKafkaSink = buildKafkaSink(TOPIC_COUPON_GET);
        KafkaSink<String> couponUseKafkaSink = buildKafkaSink(TOPIC_COUPON_USE);
        KafkaSink<String> userRegisterKafkaSink = buildKafkaSink(TOPIC_USER_REGISTER);

        favorAddStream.map(obj -> obj.toJSONString()).sinkTo(favorAddKafkaSink);
        couponGetStream.map(obj -> obj.toJSONString()).sinkTo(couponGetKafkaSink);
        couponUseStream.map(obj -> obj.toJSONString()).sinkTo(couponUseKafkaSink);
        userRegisterStream.map(obj -> obj.toJSONString()).sinkTo(userRegisterKafkaSink);

        // 10. HBase Sinks
        favorAddStream.addSink(new HBaseSink(HBASE_TABLE_FAVOR_ADD, HBASE_ZK_QUORUM));
        couponGetStream.addSink(new HBaseSink(HBASE_TABLE_COUPON_GET, HBASE_ZK_QUORUM));
        couponUseStream.addSink(new HBaseSink(HBASE_TABLE_COUPON_USE, HBASE_ZK_QUORUM));
        userRegisterStream.addSink(new HBaseSink(HBASE_TABLE_USER_REGISTER, HBASE_ZK_QUORUM));

        env.execute("DWD User Behavior Enrich & Route");
    }

    // ==================== Kafka Topic 自动创建 ====================

    private static void ensureKafkaTopicsExist() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(props)) {
            List<String> topics = Arrays.asList(TOPIC_FAVOR_ADD, TOPIC_COUPON_GET, TOPIC_COUPON_USE, TOPIC_USER_REGISTER);
            for (String topic : topics) {
                if (!topicExists(admin, topic)) {
                    createTopic(admin, topic);
                }
            }
        } catch (Exception ex) {
            System.err.println("创建 Kafka Topic 失败: " + ex.getMessage());
        }
    }

    private static boolean topicExists(AdminClient admin, String topic) {
        try {
            DescribeTopicsResult result = admin.describeTopics(Collections.singletonList(topic));
            result.values().get(topic).get(10, TimeUnit.SECONDS);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    private static void createTopic(AdminClient admin, String topic) {
        NewTopic newTopic = new NewTopic(topic, TOPIC_PARTITIONS, REPLICATION_FACTOR);
        CreateTopicsResult result = admin.createTopics(Collections.singletonList(newTopic));
        try {
            result.values().get(topic).get(30, TimeUnit.SECONDS);
            System.out.println("Kafka Topic 创建成功: " + topic);
        } catch (Exception ex) {
            System.err.println("创建 Topic 失败: " + topic + ", " + ex.getMessage());
        }
    }

    // ==================== Kafka Sink ====================

    private static KafkaSink<String> buildKafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(value -> value.getBytes(StandardCharsets.UTF_8))
                                .build()
                )
                .build();
    }

    // ==================== 路由数据载体 ====================

    public static class RoutedData {
        public String route;
        public JSONObject json;

        public RoutedData(String route, JSONObject json) {
            this.route = route;
            this.json = json;
        }
    }

    // ==================== HBase Sink ====================

    public static class HBaseSink extends RichSinkFunction<JSONObject> {
        private final String tableName;
        private final String zkQuorum;
        private transient Connection hbaseConn;
        private transient Table table;

        public HBaseSink(String tableName, String zkQuorum) {
            this.tableName = tableName;
            this.zkQuorum = zkQuorum;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", zkQuorum);
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            hbaseConn = ConnectionFactory.createConnection(conf);

            TableName tn = HBASE_NAMESPACE.isEmpty() ?
                    TableName.valueOf(tableName) :
                    TableName.valueOf(HBASE_NAMESPACE, tableName);

            table = hbaseConn.getTable(tn);
            System.out.println("HBase Sink 已连接: " + tn);
        }

        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            String rowKey = UUID.randomUUID().toString();
            System.out.println("📤 写入 HBase: " + tableName + ", rowKey=" + rowKey + ", 数据: " + value.toJSONString().substring(0, Math.min(100, value.toJSONString().length())));

            Put put = new Put(Bytes.toBytes(rowKey));
            for (Map.Entry<String, Object> entry : value.entrySet()) {
                put.addColumn(
                        Bytes.toBytes("cf"),
                        Bytes.toBytes(entry.getKey()),
                        Bytes.toBytes(entry.getValue().toString())
                );
            }
            table.put(put);
        }

        @Override
        public void close() throws Exception {
            if (table != null) {
                try { table.close(); } catch (IOException ignore) {}
            }
            if (hbaseConn != null && !hbaseConn.isClosed()) {
                try { hbaseConn.close(); } catch (IOException ignore) {}
            }
        }
    }

    // ==================== 富化 + 路由函数 ====================

    public static class EnrichAndRouteFunction extends BroadcastProcessFunction<JSONObject, List<String>, RoutedData> {

        private final MapStateDescriptor<String, List<String>> stateDesc;
        private final String hbaseZkQuorum;
        private transient Connection hbaseConn;
        private transient Map<String, Table> tableCache;

        private final Map<String, String> factToDimMap = new LinkedHashMap<>();

        public EnrichAndRouteFunction(MapStateDescriptor<String, List<String>> stateDesc, String hbaseZkQuorum) {
            this.stateDesc = stateDesc;
            this.hbaseZkQuorum = hbaseZkQuorum;

            factToDimMap.put("sku_id", "dim_sku_info");
            factToDimMap.put("spu_id", "dim_spu_info");
            factToDimMap.put("activity_id", "dim_activity_info");
            factToDimMap.put("activity_rule_id", "dim_activity_rule");
            factToDimMap.put("user_id", "dim_user_info");
            factToDimMap.put("province_id", "dim_base_province");
            factToDimMap.put("region_id", "dim_base_region");
            factToDimMap.put("category1_id", "dim_base_category1");
            factToDimMap.put("category2_id", "dim_base_category2");
            factToDimMap.put("category3_id", "dim_base_category3");
            factToDimMap.put("trademark_id", "dim_base_trademark");
            factToDimMap.put("coupon_id", "dim_coupon_info");
            factToDimMap.put("coupon_range_id", "dim_coupon_range");
            factToDimMap.put("financial_sku_id", "dim_financial_sku_cost");
        }

        @Override
        public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<RoutedData> out) throws Exception {
            if (value == null || value.isEmpty()) return;

            List<String> enabledDims = ctx.getBroadcastState(stateDesc).get("dim-tables");
            Set<String> enabled = enabledDims == null ? Collections.emptySet() : new HashSet<>(enabledDims);

            ensureHBase();

            JSONObject result = new JSONObject(value);

            // 富化维度（已修复变量名冲突）
            enrichDimensions(result, enabled);

            String route = determineRoute(result);
            if (route != null) {
                System.out.println("匹配路由: " + route + " | action_id=" + getValue(result, "action_id"));
                out.collect(new RoutedData(route, result));
            } else {
                System.out.println("未匹配路由: action_id=" + getValue(result, "action_id") + ", action_type=" + getValue(result, "action_type"));
            }
        }

        @Override
        public void processBroadcastElement(List<String> value, Context ctx, Collector<RoutedData> out) throws Exception {
            List<String> distinct = value == null ? Collections.emptyList() :
                    value.stream().distinct().collect(Collectors.toList());
            ctx.getBroadcastState(stateDesc).put("dim-tables", distinct);
            System.out.println("维度表更新: " + distinct);
        }

        // 修复：变量名冲突（e -> entry），异常 -> ex
        private void enrichDimensions(JSONObject result, Set<String> enabled) {
            for (Map.Entry<String, String> entry : factToDimMap.entrySet()) {
                String fkField = entry.getKey();
                String dimTable = entry.getValue();

                if (!enabled.isEmpty() && !enabled.contains(dimTable)) continue;

                String idVal = result.getString(fkField);
                if (idVal == null || idVal.isEmpty()) continue;

                try {
                    JSONObject dim = getRowAsJson(dimTable, idVal);
                    if (dim != null && !dim.isEmpty()) {
                        for (Map.Entry<String, Object> col : dim.entrySet()) {
                            result.put(dimTable + "_" + col.getKey(), col.getValue());
                        }
                    }
                } catch (Exception ex) {
                    System.err.println("HBase 查询失败: table=" + dimTable + ", id=" + idVal + ", " + ex.getMessage());
                }
            }
        }

        private String determineRoute(JSONObject json) {
            String actionId = getValue(json, "action_id");
            String actionType = getValue(json, "action_type");

            if ("favor_add".equals(actionId) || "collect".equals(actionId) || "collect".equals(actionType)) {
                return "favor_add";
            } else if ("coupon_get".equals(actionId) || "get_coupon".equals(actionType)) {
                return "coupon_get";
            } else if ("coupon_use".equals(actionId) || "use_coupon".equals(actionType)) {
                return "coupon_use";
            } else if ("user_register".equals(actionId) || "register".equals(actionType)) {
                return "user_register";
            }

            return null;
        }

        private String getValue(JSONObject json, String key) {
            JSONObject action = json.getJSONObject("event_detail") != null ?
                    json.getJSONObject("event_detail").getJSONObject("action") : null;
            return action != null ? action.getString(key) : null;
        }

        private void ensureHBase() {
            try {
                if (hbaseConn == null || hbaseConn.isClosed()) {
                    org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                    conf.set("hbase.zookeeper.quorum", hbaseZkQuorum);
                    conf.set("hbase.zookeeper.property.clientPort", "2181");
                    hbaseConn = ConnectionFactory.createConnection(conf);
                    tableCache = new ConcurrentHashMap<>();
                }
            } catch (Exception ex) {
                System.err.println("HBase 连接失败: " + ex.getMessage());
            }
        }

        private Table getTable(String pureTableName) {
            try {
                String full = HBASE_NAMESPACE.isEmpty() ? pureTableName : HBASE_NAMESPACE + ":" + pureTableName;
                Table table = tableCache.get(full);
                if (table == null) {
                    table = hbaseConn.getTable(TableName.valueOf(full));
                    tableCache.put(full, table);
                }
                return table;
            } catch (Exception ex) {
                System.err.println("获取 HBase Table 失败: " + pureTableName + ", " + ex.getMessage());
                return null;
            }
        }

        private JSONObject getRowAsJson(String pureTableName, String rowKey) {
            try {
                Table table = getTable(pureTableName);
                if (table == null) return null;

                Get get = new Get(Bytes.toBytes(rowKey));
                Result res = table.get(get);
                if (res == null || res.isEmpty()) return null;

                JSONObject json = new JSONObject();
                for (Cell cell : res.listCells()) {
                    String qualifier = Bytes.toString(cell.getQualifierArray(),
                            cell.getQualifierOffset(), cell.getQualifierLength());
                    String val = Bytes.toString(cell.getValueArray(),
                            cell.getValueOffset(), cell.getValueLength());
                    json.put(qualifier, val);
                }
                return json;
            } catch (Exception ex) {
                System.err.println("HBase 查询失败: table=" + pureTableName + ", rowKey=" + rowKey + ", " + ex.getMessage());
                return null;
            }
        }

        @Override
        public void close() throws Exception {
            if (tableCache != null) {
                for (Table t : tableCache.values()) {
                    try { t.close(); } catch (Exception ignore) {}
                }
                tableCache.clear();
            }
            if (hbaseConn != null && !hbaseConn.isClosed()) {
                hbaseConn.close();
            }
        }
    }
}