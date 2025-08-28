package dby.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DwdAsyncJob {
    public static void main(String[] args) throws Exception {
        // 1. Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Kafka 配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "cdh01:9092");
        props.setProperty("group.id", "dwd_async_job");

        // Source: ODS Kafka -> DataStream
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>("work_order_05", new SimpleStringSchema(), props);

        // ⭐ 从最早 offset 开始消费
        consumer.setStartFromEarliest();

        DataStream<String> source = env.addSource(consumer);

        // ⭐ 先打印原始 Kafka 数据，确认消费正常
        source.print("Kafka >>> ");

        DataStream<JSONObject> jsonStream = source.map(JSON::parseObject);

        // ===================== 维度异步 Join =====================
        // 订单明细
        DataStream<JSONObject> orderItemWithDim = AsyncDataStream.unorderedWait(
                jsonStream.filter(x -> "order_item".equals(x.getString("event_type"))),
                new HBaseAsyncLookup("dim_item_05", "item_id", "f"),
                60, TimeUnit.SECONDS, 100);

        orderItemWithDim
                .map(x -> x.toJSONString())
                .addSink(new FlinkKafkaProducer<>("dwd_order_item_05", new SimpleStringSchema(), props));
        orderItemWithDim.map(x -> x.toJSONString()).print();

        // 支付
        DataStream<JSONObject> paymentWithDim = AsyncDataStream.unorderedWait(
                jsonStream.filter(x -> "payment".equals(x.getString("event_type"))),
                new HBaseAsyncLookup("dim_user_05", "buyer_id", "f"),
                60, TimeUnit.SECONDS, 100);

        paymentWithDim
                .map(x -> x.toJSONString())
                .addSink(new FlinkKafkaProducer<>("dwd_payment_05", new SimpleStringSchema(), props));
        paymentWithDim.map(x -> x.toJSONString()).print();

        // 退款
        DataStream<JSONObject> refundWithDim = AsyncDataStream.unorderedWait(
                jsonStream.filter(x -> "refund".equals(x.getString("event_type"))),
                new HBaseAsyncLookup("dim_user_05", "buyer_id", "f"),
                60, TimeUnit.SECONDS, 100);

        refundWithDim
                .map(x -> x.toJSONString())
                .addSink(new FlinkKafkaProducer<>("dwd_refund_05", new SimpleStringSchema(), props));
        refundWithDim.map(x -> x.toJSONString()).print();

        // 用户
        DataStream<JSONObject> userWithDim = AsyncDataStream.unorderedWait(
                jsonStream.filter(x -> "user".equals(x.getString("event_type"))),
                new HBaseAsyncLookup("dim_user_05", "user_id", "f"),
                60, TimeUnit.SECONDS, 100);

        userWithDim
                .map(x -> x.toJSONString())
                .addSink(new FlinkKafkaProducer<>("dwd_user_05", new SimpleStringSchema(), props));
        userWithDim.map(x -> x.toJSONString()).print();

        // 商品
        DataStream<JSONObject> itemWithDim = AsyncDataStream.unorderedWait(
                jsonStream.filter(x -> "item".equals(x.getString("event_type"))),
                new HBaseAsyncLookup("dim_item_05", "item_id", "f"),
                60, TimeUnit.SECONDS, 100);

        itemWithDim
                .map(x -> x.toJSONString())
                .addSink(new FlinkKafkaProducer<>("dwd_item_05", new SimpleStringSchema(), props));
        itemWithDim.map(x -> x.toJSONString()).print();

        // 店铺
        DataStream<JSONObject> shopWithDim = AsyncDataStream.unorderedWait(
                jsonStream.filter(x -> "shop".equals(x.getString("event_type"))),
                new HBaseAsyncLookup("dim_shop_05", "shop_id", "f"),
                60, TimeUnit.SECONDS, 100);

        shopWithDim
                .map(x -> x.toJSONString())
                .addSink(new FlinkKafkaProducer<>("dwd_shop_05", new SimpleStringSchema(), props));
        shopWithDim.map(x -> x.toJSONString()).print();

        // SKU
        DataStream<JSONObject> skuWithDim = AsyncDataStream.unorderedWait(
                jsonStream.filter(x -> "sku".equals(x.getString("event_type"))),
                new HBaseAsyncLookup("dim_sku_05", "sku_id", "f"),
                60, TimeUnit.SECONDS, 100);

        skuWithDim
                .map(x -> x.toJSONString())
                .addSink(new FlinkKafkaProducer<>("dwd_sku_05", new SimpleStringSchema(), props));
        skuWithDim.map(x -> x.toJSONString()).print();

        // 日期
        DataStream<JSONObject> dateWithDim = AsyncDataStream.unorderedWait(
                jsonStream.filter(x -> "date".equals(x.getString("event_type"))),
                new HBaseAsyncLookup("dim_date_05", "date_id", "f"),
                60, TimeUnit.SECONDS, 100);

        dateWithDim
                .map(x -> x.toJSONString())
                .addSink(new FlinkKafkaProducer<>("dwd_date_05", new SimpleStringSchema(), props));
        dateWithDim.map(x -> x.toJSONString()).print();

        // 类目
        DataStream<JSONObject> categoryWithDim = AsyncDataStream.unorderedWait(
                jsonStream.filter(x -> "category".equals(x.getString("event_type"))),
                new HBaseAsyncLookup("dim_category_05", "category_id", "f"),
                60, TimeUnit.SECONDS, 100);

        categoryWithDim
                .map(x -> x.toJSONString())
                .addSink(new FlinkKafkaProducer<>("dwd_category_05", new SimpleStringSchema(), props));
        categoryWithDim.map(x -> x.toJSONString()).print();

        // 执行任务
        env.execute("ODS -> DWD Async Join with All HBase Dimensions");
    }
}

// ====================== HBase 异步维度查询类 ======================
class HBaseAsyncLookup extends RichAsyncFunction<JSONObject, JSONObject> {
    private final String tableName;
    private final String keyField;
    private final String family;

    private transient org.apache.hadoop.hbase.client.Connection conn;
    private transient Table table;
    private transient ExecutorService executor;

    public HBaseAsyncLookup(String tableName, String keyField, String family) {
        this.tableName = tableName;
        this.keyField = keyField;
        this.family = family;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "cdh01");
        conn = ConnectionFactory.createConnection(config);
        table = conn.getTable(TableName.valueOf(tableName));
        executor = Executors.newFixedThreadPool(10);
    }

    @Override
    public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) {
        executor.submit(() -> {
            try {
                String key = input.getString(keyField);
                if (key == null) {
                    resultFuture.complete(Collections.singleton(input));
                    return;
                }
                Get get = new Get(key.getBytes());
                Result result = table.get(get);
                if (!result.isEmpty()) {
                    result.listCells().forEach(cell -> {
                        String qualifier = new String(cell.getQualifierArray(),
                                cell.getQualifierOffset(), cell.getQualifierLength());
                        String value = new String(cell.getValueArray(),
                                cell.getValueOffset(), cell.getValueLength());
                        input.put(qualifier, value);
                    });
                }
                resultFuture.complete(Collections.singleton(input));
            } catch (Exception e) {
                e.printStackTrace();
                resultFuture.complete(Collections.singleton(input));
            }
        });
    }

    @Override
    public void close() throws Exception {
        table.close();
        conn.close();
        executor.shutdown();
    }
}
