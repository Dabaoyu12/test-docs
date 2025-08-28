package dby.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.*;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.*;


/**
 * DWS 聚合层（演示版）
 *
 * 功能（每个子流输出到独立 Kafka topic 并打印）：
 *  - dws_item_pv_05    : item_id, window_start, window_end, pv_cnt
 *  - dws_item_uv_05    : item_id, window_start, window_end, uv_cnt
 *  - dws_item_cart_05  : item_id, window_start, window_end, cart_qty_sum
 *  - dws_item_pay_05   : item_id, window_start, window_end, pay_amt_sum
 *  - dws_item_pay_buyer_cnt_05 : item_id, window_start, window_end, pay_buyer_cnt (去重)
 *  - dws_item_order_buyer_cnt_05: item_id, window_start, window_end, order_buyer_cnt (下单买家数)
 *  - dws_item_pay_conv_05 : item_id, window_start, window_end, pay_buyer_cnt / uv_cnt (保护除零)
 *
 * 实现细节：
 *  - 使用 event_time（从 JSON 的 event_time 字段解析）
 *  - 窗口：5 分钟滚动窗口（可修改）
 *  - payment -> item 需要 order_item 明细，示例用 HBase 异步查 "order_items" 表（你需保证该表存在或改成别的 join）
 *  - 所有输出同时写入 Kafka 和 print()
 */
public class DwsAsyncAggJob {

    // Kafka bootstrap
    private static final String KAFKA_BOOTSTRAP = "cdh01:9092";

    // Window size
    private static final Time WINDOW_SIZE = Time.minutes(5);

    // HBase order_items table (假设)
    // rowkey = order_id, family = "cf", qualifiers: item:<idx>:item_id, item:<idx>:qty, item:<idx>:unit_price
    private static final String HBASE_ORDER_ITEMS_TABLE = "order_items";

    public static void main(String[] args) throws Exception {
        // 1) Flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 2) Kafka consumer props
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP);
        props.setProperty("group.id", "dws_agg_job_group");

        // 3) ensure sink topics
        List<String> sinkTopics = Arrays.asList(
                "dws_item_pv_05",
                "dws_item_uv_05",
                "dws_item_cart_05",
                "dws_item_pay_05",
                "dws_item_pay_buyer_cnt_05",
                "dws_item_order_buyer_cnt_05",
                "dws_item_pay_conv_05"
        );
        createKafkaTopicsIfNotExist(KAFKA_BOOTSTRAP, sinkTopics);

        // 4) build sources: we assume these DWD topics exist and contain JSON objects with event_time field
        FlinkKafkaConsumer<String> pageConsumer = new FlinkKafkaConsumer<>("dwd_page_log_05", new SimpleStringSchema(), props);
        pageConsumer.setStartFromEarliest();
        DataStream<String> pageSrc = env.addSource(pageConsumer);

        FlinkKafkaConsumer<String> cartConsumer = new FlinkKafkaConsumer<>("dwd_cart_add_05", new SimpleStringSchema(), props);
        cartConsumer.setStartFromEarliest();
        DataStream<String> cartSrc = env.addSource(cartConsumer);

        FlinkKafkaConsumer<String> paymentConsumer = new FlinkKafkaConsumer<>("dwd_payment_05", new SimpleStringSchema(), props);
        paymentConsumer.setStartFromEarliest();
        DataStream<String> paymentSrc = env.addSource(paymentConsumer);

        FlinkKafkaConsumer<String> orderConsumer = new FlinkKafkaConsumer<>("dwd_order_05", new SimpleStringSchema(), props);
        orderConsumer.setStartFromEarliest();
        DataStream<String> orderSrc = env.addSource(orderConsumer);

        FlinkKafkaConsumer<String> orderItemConsumer = new FlinkKafkaConsumer<>("dwd_order_item_05", new SimpleStringSchema(), props);
        orderItemConsumer.setStartFromEarliest();
        DataStream<String> orderItemSrc = env.addSource(orderItemConsumer);

        // 5) parse JSON and assign timestamps/watermarks (we use event_time string field "event_time" or pay_time/order_time)
        DataStream<JSONObject> pageJson = pageSrc
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(jsonTsExtractor("event_time"));

        DataStream<JSONObject> cartJson = cartSrc
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(jsonTsExtractor("event_time"));

        DataStream<JSONObject> paymentJson = paymentSrc
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(jsonTsExtractor("pay_time"));

        DataStream<JSONObject> orderJson = orderSrc
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(jsonTsExtractor("order_time"));

        DataStream<JSONObject> orderItemJson = orderItemSrc
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(jsonTsExtractor("ts")); // if ts present

        // ==================== PV (按 item) ====================
        SingleOutputStreamOperator<JSONObject> itemPv = pageJson
                .filter(j -> {
                    String et = j.getString("event_type");
                    return et != null && Arrays.asList("page_view", "detail_view", "video_view", "live_view").contains(et);
                })
                .keyBy(j -> j.getString("item_id") == null ? "NULL_ITEM" : j.getString("item_id"))
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .process(new CountWindowFunction("pv_cnt"));

        // sink pv
        sinkAndPrint(itemPv, "dws_item_pv_05", props);

        // ==================== UV (按 item, 窗口内对 user_id 去重) ====================
        SingleOutputStreamOperator<JSONObject> itemUv = pageJson
                .filter(j -> {
                    String et = j.getString("event_type");
                    return et != null && Arrays.asList("page_view", "detail_view", "video_view", "live_view").contains(et);
                })
                .keyBy(j -> j.getString("item_id") == null ? "NULL_ITEM" : j.getString("item_id"))
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .process(new UniqueCountWindowFunction("user_id", "uv_cnt"));

        sinkAndPrint(itemUv, "dws_item_uv_05", props);

        // ==================== Cart add (按 item sum qty) ====================
        SingleOutputStreamOperator<JSONObject> itemCart = cartJson
                .filter(j -> "add_to_cart".equals(j.getString("event_type")) || j.containsKey("qty"))
                .keyBy(j -> j.getString("item_id") == null ? "NULL_ITEM" : j.getString("item_id"))
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .process(new SumLongFieldWindowFunction("qty", "cart_qty_sum"));

        sinkAndPrint(itemCart, "dws_item_cart_05", props);

        // ==================== Payment -> 转成 item 级别 (异步从 HBase 查订单明细) ====================
        // 对 payment 流做异步 lookup order_items 表，将 payment 拆成多条 item 级别事件 (含 buyer_id/pay_amount 等)
        DataStream<JSONObject> paymentEnriched = AsyncDataStream.unorderedWait(
                paymentJson,
                new PaymentOrderItemsAsyncLookup(HBASE_ORDER_ITEMS_TABLE, "order_id"),
                60, TimeUnit.SECONDS, 100);

        // 聚合支付金额按 item
        SingleOutputStreamOperator<JSONObject> itemPay = paymentEnriched
                .keyBy(j -> j.getString("item_id") == null ? "NULL_ITEM" : j.getString("item_id"))
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .process(new SumDoubleFieldWindowFunction("pay_share_amount", "pay_amt_sum"));

        sinkAndPrint(itemPay, "dws_item_pay_05", props);

        // 聚合支付买家数（去重 buyer_id）
        SingleOutputStreamOperator<JSONObject> itemPayBuyerCnt = paymentEnriched
                .keyBy(j -> j.getString("item_id") == null ? "NULL_ITEM" : j.getString("item_id"))
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .process(new UniqueCountWindowFunction("buyer_id", "pay_buyer_cnt"));

        sinkAndPrint(itemPayBuyerCnt, "dws_item_pay_buyer_cnt_05", props);

        // ==================== 下单买家数：order -> async lookup order_items -> item 级别（去重 buyer_id） ====================
        DataStream<JSONObject> orderEnriched = AsyncDataStream.unorderedWait(
                orderJson,
                new OrderItemsAsyncLookup(HBASE_ORDER_ITEMS_TABLE, "order_id"),
                60, TimeUnit.SECONDS, 100);

        SingleOutputStreamOperator<JSONObject> itemOrderBuyerCnt = orderEnriched
                .keyBy(j -> j.getString("item_id") == null ? "NULL_ITEM" : j.getString("item_id"))
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .process(new UniqueCountWindowFunction("buyer_id", "order_buyer_cnt"));

        sinkAndPrint(itemOrderBuyerCnt, "dws_item_order_buyer_cnt_05", props);

        // ==================== 支付转化率：需要 join itemPayBuyerCnt 和 itemUv on (item_id, window) ->
        // 为简化：我们把两个流写出到 Kafka，然后在 ADS 层做 Join；这里也演示本地 join（窗口内缓存）较复杂，建议在 ADS 做 TopN/比率计算。
        // 这里我们简单做一个 local join: 当 payBuyer 和 uv arrive we compute ratio — this is a simplified demo.

        // For demo compute pay_conv per item by co-grouping two streams by item & window using a simple cache map (not production-grade)
        // We'll key by item and use ProcessWindowFunction? To avoid complexity we write both streams to Kafka and let ADS compute actual ratio.
        // But we'll also produce a naive local join: combine last known uv and pay_buyer_cnt per item in a keyed state (best-effort).
        SingleOutputStreamOperator<JSONObject> payConvNaive = joinLatestCounts(env, itemPayBuyerCnt, itemUv, "pay_buyer_cnt", "uv_cnt", "pay_conv");

        sinkAndPrint(payConvNaive, "dws_item_pay_conv_05", props);

        // 6) start job
        env.execute("DWS Aggregation Job (Async IO + HBase lookup) - 05");
    }

    // ---------- Utilities ----------

    private static WatermarkStrategy<JSONObject> jsonTsExtractor(String timeField) {
        return WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner((event, ts) -> {
                    try {
                        String t = event.getString(timeField);
                        if (t == null) {
                            // try common alternatives
                            t = event.getString("event_time");
                        }
                        if (t == null) return System.currentTimeMillis();
                        // 支持 "yyyy-MM-dd HH:mm:ss[.SSS]" 格式
                        // 为简化直接 parse to epoch millis using java.time
                        java.time.format.DateTimeFormatter f = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]");
                        java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(t, f);
                        long epoch = ldt.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
                        return epoch;
                    } catch (Exception e) {
                        return System.currentTimeMillis();
                    }
                });
    }

    private static void sinkAndPrint(DataStream<JSONObject> stream, String topic, Properties kafkaProps) {
        // print to console
        stream.map(JSONObject::toJSONString).print();

        // write to kafka
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                topic,
                new SimpleStringSchema(),
                kafkaProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
        stream.map(JSONObject::toJSONString).addSink(producer);
    }

    /**
     * Create Kafka topics if not exist
     */
    private static void createKafkaTopicsIfNotExist(String bootstrapServers, List<String> topics) {
        Properties conf = new Properties();
        conf.put("bootstrap.servers", bootstrapServers);
        try (AdminClient admin = AdminClient.create(conf)) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> toCreate = new ArrayList<>();
            for (String t : topics) {
                if (!existing.contains(t)) {
                    System.out.println("Create topic: " + t);
                    toCreate.add(new NewTopic(t, 1, (short) 1));
                } else {
                    System.out.println("Topic exists: " + t);
                }
            }
            if (!toCreate.isEmpty()) {
                admin.createTopics(toCreate).all().get(30, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            System.err.println("Failed to create topics: " + e.getMessage());
        }
    }

    // =============== Window helper functions ===============

    /**
     * Generic process window function that counts elements in window and emits JSON
     * expects keyed stream keyed by id (item_id)
     */
    static class CountWindowFunction extends ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow> {
        private final String outField;

        CountWindowFunction(String outField) {
            this.outField = outField;
        }

        @Override
        public void process(String key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) {
            long cnt = 0;
            for (JSONObject ignored : elements) cnt++;
            JSONObject result = new JSONObject();
            result.put("id", key);
            result.put("window_start", context.window().getStart());
            result.put("window_end", context.window().getEnd());
            result.put(outField, cnt);
            returnValueCommonFields(result);
            out.collect(result);
        }
    }

    /**
     * ProcessWindowFunction that collects distinct values of given field in window
     * emits count as outField
     */
    static class UniqueCountWindowFunction extends ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow> {
        private final String field;
        private final String outField;

        UniqueCountWindowFunction(String field, String outField) {
            this.field = field;
            this.outField = outField;
        }

        @Override
        public void process(String key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) {
            Set<String> uniq = new HashSet<>();
            for (JSONObject j : elements) {
                String v = j.getString(field);
                if (v != null) uniq.add(v);
            }
            JSONObject result = new JSONObject();
            result.put("id", key);
            result.put("window_start", context.window().getStart());
            result.put("window_end", context.window().getEnd());
            result.put(outField, uniq.size());
            returnValueCommonFields(result);
            out.collect(result);
        }
    }

    /**
     * Sum numeric long field in window
     */
    static class SumLongFieldWindowFunction extends ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow> {
        private final String field;
        private final String outField;

        SumLongFieldWindowFunction(String field, String outField) {
            this.field = field;
            this.outField = outField;
        }

        @Override
        public void process(String key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) {
            long sum = 0;
            for (JSONObject j : elements) {
                try {
                    Number n = (Number) j.get(field);
                    if (n == null) n = j.getLong(field);
                    if (n != null) sum += n.longValue();
                } catch (Exception ignored) {
                    // try parse string
                    try {
                        sum += Long.parseLong(String.valueOf(j.get(field)));
                    } catch (Exception ex) { }
                }
            }
            JSONObject result = new JSONObject();
            result.put("id", key);
            result.put("window_start", context.window().getStart());
            result.put("window_end", context.window().getEnd());
            result.put(outField, sum);
            returnValueCommonFields(result);
            out.collect(result);
        }
    }

    /**
     * Sum numeric double field in window
     */
    static class SumDoubleFieldWindowFunction extends ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow> {
        private final String field;
        private final String outField;

        SumDoubleFieldWindowFunction(String field, String outField) {
            this.field = field;
            this.outField = outField;
        }

        @Override
        public void process(String key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) {
            double sum = 0.0;
            for (JSONObject j : elements) {
                try {
                    Number n = (Number) j.get(field);
                    if (n != null) sum += n.doubleValue();
                    else {
                        Object o = j.get(field);
                        if (o != null) sum += Double.parseDouble(o.toString());
                    }
                } catch (Exception ignored) { }
            }
            JSONObject result = new JSONObject();
            result.put("id", key);
            result.put("window_start", context.window().getStart());
            result.put("window_end", context.window().getEnd());
            result.put(outField, sum);
            returnValueCommonFields(result);
            out.collect(result);
        }
    }

    private static void returnValueCommonFields(JSONObject obj) {
        obj.put("ts", System.currentTimeMillis());
    }

    // =============== Naive join latest counts (demo) ===============
    /**
     * Very small demo: join two keyed streams of per-item aggregated counts (both JSON with id, window_start, window_end, count fields)
     * This is a simplistic best-effort join that keeps last known values in keyed state (not window-coordinated; production code should use proper window join)
     */
    private static SingleOutputStreamOperator<JSONObject> joinLatestCounts(StreamExecutionEnvironment env,
                                                                           DataStream<JSONObject> leftCounts,
                                                                           DataStream<JSONObject> rightCounts,
                                                                           String leftField, String rightField, String outField) {
        // key by id
        KeyedStream<JSONObject, String> leftKeyed = leftCounts.keyBy(j -> j.getString("id"));
        KeyedStream<JSONObject, String> rightKeyed = rightCounts.keyBy(j -> j.getString("id"));

        // Convert to broadcast-like small keyed streams by unioning with markers
        DataStream<Tuple2<String, JSONObject>> leftTagged = leftKeyed.map((MapFunction<JSONObject, Tuple2<String, JSONObject>>) j -> Tuple2.of(j.getString("id"), j));
        DataStream<Tuple2<String, JSONObject>> rightTagged = rightKeyed.map((MapFunction<JSONObject, Tuple2<String, JSONObject>>) j -> Tuple2.of(j.getString("id"), j));

        // union and process keyed
        DataStream<Tuple2<String, JSONObject>> unioned = leftTagged.union(rightTagged);

        return unioned
                .keyBy(t -> t.f0)
                .process(new org.apache.flink.streaming.api.functions.KeyedProcessFunction<String, Tuple2<String, JSONObject>, JSONObject>() {
                    private transient Map<String, JSONObject> lastLeft = new HashMap<>();
                    private transient Map<String, JSONObject> lastRight = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastLeft = new HashMap<>();
                        lastRight = new HashMap<>();
                    }

                    @Override
                    public void processElement(Tuple2<String, JSONObject> value, Context ctx, Collector<JSONObject> out) {
                        JSONObject j = value.f1;
                        // determine whether this is left or right by field presence
                        if (j.containsKey(leftField)) {
                            lastLeft.put(value.f0, j);
                        }
                        if (j.containsKey(rightField)) {
                            lastRight.put(value.f0, j);
                        }
                        JSONObject l = lastLeft.get(value.f0);
                        JSONObject r = lastRight.get(value.f0);
                        if (l != null && r != null) {
                            // compute ratio
                            double leftVal = l.getDoubleValue(leftField);
                            double rightVal = r.getDoubleValue(rightField);
                            double conv = (rightVal == 0.0) ? 0.0 : leftVal / rightVal;
                            JSONObject outj = new JSONObject();
                            outj.put("id", value.f0);
                            outj.put("window_start", Math.max(l.getLongValue("window_start"), r.getLongValue("window_start")));
                            outj.put("window_end", Math.min(l.getLongValue("window_end"), r.getLongValue("window_end")));
                            outj.put(leftField, leftVal);
                            outj.put(rightField, rightVal);
                            outj.put(outField, conv);
                            outj.put("ts", System.currentTimeMillis());
                            out.collect(outj);
                        }
                    }
                });
    }

    // =============== Async HBase Lookup for payment -> order items ===============
    /**
     * PaymentOrderItemsAsyncLookup:
     *  - input: JSONObject payment {order_id, payment_id, pay_amount, buyer_id, pay_time ...}
     *  - async lookup order_items in HBase (rowkey order_id) and produce one enriched JSONObject per item:
     *      {order_id, payment_id, buyer_id, item_id, qty, unit_price, pay_share_amount, pay_time}
     *
     *  NOTE: This implementation assumes HBase rows formatted in a simple manner: family "cf" with qualifiers like:
     *      item_1:id -> "item_831"
     *      item_1:qty -> "2"
     *      item_1:unit_price -> "11.06"
     *
     *  You must ensure real order_items are available or replace this logic with a proper join.
     */
    static class PaymentOrderItemsAsyncLookup extends RichAsyncFunction<JSONObject, JSONObject> {
        private final String hbaseTable;
        private final String orderIdField;

        private transient org.apache.hadoop.hbase.client.Connection conn;
        private transient Table table;
        private transient ExecutorService executor;

        PaymentOrderItemsAsyncLookup(String hbaseTable, String orderIdField) {
            this.hbaseTable = hbaseTable;
            this.orderIdField = orderIdField;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "cdh01");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(hbaseTable));
            executor = Executors.newFixedThreadPool(10);
        }

        @Override
        public void asyncInvoke(JSONObject payment, ResultFuture<JSONObject> resultFuture) throws Exception {
            executor.submit(() -> {
                String orderId = payment.getString(orderIdField);
                if (orderId == null) {
                    resultFuture.complete(Collections.singleton(payment));
                    return;
                }
                try {
                    Get g = new Get(Bytes.toBytes(orderId));
                    Result r = table.get(g);
                    if (r == null || r.isEmpty()) {
                        // no order items found -> emit payment unchanged (or drop)
                        // We emit as-is but without item info so it won't be aggregated at item-level
                        resultFuture.complete(Collections.singleton(payment));
                        return;
                    }
                    // parse cells: group by item idx prefixes
                    Map<String, Map<String, String>> items = new HashMap<>();
                    for (Cell cell : r.listCells()) {
                        String qual = Bytes.toString(CellUtil.cloneQualifier(cell));
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        // expected format: item_1:id  item_1:qty item_1:unit_price
                        String[] parts = qual.split(":", 2);
                        if (parts.length == 2) {
                            String prefix = parts[0]; // item_1
                            String q = parts[1]; // id | qty | unit_price
                            items.computeIfAbsent(prefix, k -> new HashMap<>()).put(q, value);
                        }
                    }
                    // build one enriched JSONObject per item and proportionally allocate payment amount (naive equally split if unit_price missing)
                    List<JSONObject> outList = new ArrayList<>();
                    double totalLineAmount = 0.0;
                    List<JSONObject> lines = new ArrayList<>();
                    for (Map<String, String> line : items.values()) {
                        String itemId = line.get("id");
                        if (itemId == null) continue;
                        int qty = 1;
                        double unitPrice = 0.0;
                        try { if (line.get("qty") != null) qty = Integer.parseInt(line.get("qty")); } catch (Exception e) {}
                        try { if (line.get("unit_price") != null) unitPrice = Double.parseDouble(line.get("unit_price")); } catch (Exception e) {}
                        double lineAmt = qty * unitPrice;
                        totalLineAmount += lineAmt;
                        JSONObject ln = new JSONObject();
                        ln.put("item_id", itemId);
                        ln.put("qty", qty);
                        ln.put("unit_price", unitPrice);
                        ln.put("line_amount", lineAmt);
                        lines.add(ln);
                    }
                    double payAmount = payment.getDoubleValue("pay_amount");
                    if (totalLineAmount <= 0.0) {
                        // cannot proportionally split -> naive split by item count
                        int n = lines.size() == 0 ? 1 : lines.size();
                        double each = payAmount / n;
                        for (JSONObject ln : lines) {
                            JSONObject out = new JSONObject(payment);
                            out.put("item_id", ln.getString("item_id"));
                            out.put("qty", ln.getInteger("qty"));
                            out.put("unit_price", ln.getDouble("unit_price"));
                            out.put("pay_share_amount", each);
                            outList.add(out);
                        }
                    } else {
                        for (JSONObject ln : lines) {
                            double share = (ln.getDoubleValue("line_amount") / totalLineAmount) * payAmount;
                            JSONObject out = new JSONObject(payment);
                            out.put("item_id", ln.getString("item_id"));
                            out.put("qty", ln.getInteger("qty"));
                            out.put("unit_price", ln.getDouble("unit_price"));
                            out.put("pay_share_amount", share);
                            outList.add(out);
                        }
                    }
                    if (outList.isEmpty()) {
                        resultFuture.complete(Collections.singleton(payment));
                    } else {
                        resultFuture.complete(outList);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    resultFuture.complete(Collections.singleton(payment));
                }
            });
        }

        @Override
        public void timeout(JSONObject input, ResultFuture<JSONObject> resultFuture) {
            // on timeout, just pass input through
            resultFuture.complete(Collections.singleton(input));
        }

        @Override
        public void close() throws Exception {
            if (table != null) table.close();
            if (conn != null) conn.close();
            if (executor != null) executor.shutdown();
        }
    }

    /**
     * Similar async lookup for order -> items (used for order -> item mapping to count order buyers)
     */
    static class OrderItemsAsyncLookup extends PaymentOrderItemsAsyncLookup {
        OrderItemsAsyncLookup(String hbaseTable, String orderIdField) {
            super(hbaseTable, orderIdField);
        }
    }
}
