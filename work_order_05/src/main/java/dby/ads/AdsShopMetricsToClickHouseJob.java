package dby.ads;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;


public class AdsShopMetricsToClickHouseJob {

    // ===== Kafka =====
    private static final String KAFKA_BOOTSTRAP = "cdh01:9092";

    private static final String TOPIC_PAGE   = "ads_shop_daily";
    private static final String TOPIC_ORDER  = "ads_shop_item_cart_daily";
    private static final String TOPIC_PAY    = "ads_shop_item_daily";
    private static final String TOPIC_REFUND = "dwd_refund_detail";
    private static final String TOPIC_CART   = "dwd_cart_add"; // 预留

    // ===== ClickHouse =====
    private static final String CK_URL      = "jdbc:clickhouse://cdh01:8123/work_order_05";
    private static final String CK_DRIVER   = "ru.yandex.clickhouse.ClickHouseDriver";
    private static final String CK_USER     = "root";
    private static final String CK_PASSWORD = "123456";

    // ===== Window / WM =====
    private static final String WINDOW_SIZE_DAYS    = "1";   // 日窗
    private static final String WATERMARK_DELAY_MIN = "30";  // 迟到 30 分钟

    public static void main(String[] args) throws Exception {
        // ---------- Runtime env ----------
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
//        env.enableCheckpointing(0); // 需要可以改为 5min，并配置状态后端
        env.setRestartStrategy(RestartStrategies.noRestart());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());

        // ---------- Kafka Sources (Table) ----------
        tEnv.executeSql(
                "CREATE TABLE page_src (\n" +
                        "  shop_id STRING,\n" +
                        "  item_id STRING,\n" +
                        "  user_id STRING,\n" +
                        "  event_time TIMESTAMP(3),\n" +
                        "  WATERMARK FOR event_time AS event_time - INTERVAL '" + WATERMARK_DELAY_MIN + "' MINUTE\n" +
                        ") WITH (\n" +
                        "  'connector'='kafka',\n" +
                        "  'topic'='" + TOPIC_PAGE + "',\n" +
                        "  'properties.bootstrap.servers'='" + KAFKA_BOOTSTRAP + "',\n" +
                        "  'properties.group.id'='ads_page',\n" +
                        "  'scan.startup.mode'='latest-offset',\n" +
                        "  'format'='json',\n" +
                        "  'json.ignore-parse-errors'='true'\n" +
                        ")");

        tEnv.executeSql(
                "CREATE TABLE order_src (\n" +
                        "  shop_id STRING,\n" +
                        "  item_id STRING,\n" +
                        "  buyer_id STRING,\n" +
                        "  order_id STRING,\n" +
                        "  order_time TIMESTAMP(3),\n" +
                        "  WATERMARK FOR order_time AS order_time - INTERVAL '" + WATERMARK_DELAY_MIN + "' MINUTE\n" +
                        ") WITH (\n" +
                        "  'connector'='kafka',\n" +
                        "  'topic'='" + TOPIC_ORDER + "',\n" +
                        "  'properties.bootstrap.servers'='" + KAFKA_BOOTSTRAP + "',\n" +
                        "  'properties.group.id'='ads_order',\n" +
                        "  'scan.startup.mode'='latest-offset',\n" +
                        "  'format'='json',\n" +
                        "  'json.ignore-parse-errors'='true'\n" +
                        ")");

        tEnv.executeSql(
                "CREATE TABLE pay_src (\n" +
                        "  shop_id STRING,\n" +
                        "  item_id STRING,\n" +
                        "  buyer_id STRING,\n" +
                        "  order_id STRING,\n" +
                        "  pay_amount DECIMAL(20,2),\n" +
                        "  pay_status STRING,\n" +
                        "  is_presale_tail_paid BOOLEAN,\n" +
                        "  is_zero_order_afterpay BOOLEAN,\n" +
                        "  pay_time TIMESTAMP(3),\n" +
                        "  WATERMARK FOR pay_time AS pay_time - INTERVAL '" + WATERMARK_DELAY_MIN + "' MINUTE\n" +
                        ") WITH (\n" +
                        "  'connector'='kafka',\n" +
                        "  'topic'='" + TOPIC_PAY + "',\n" +
                        "  'properties.bootstrap.servers'='" + KAFKA_BOOTSTRAP + "',\n" +
                        "  'properties.group.id'='ads_pay',\n" +
                        "  'scan.startup.mode'='latest-offset',\n" +
                        "  'format'='json',\n" +
                        "  'json.ignore-parse-errors'='true'\n" +
                        ")");

        tEnv.executeSql(
                "CREATE TABLE refund_src (\n" +
                        "  shop_id STRING,\n" +
                        "  item_id STRING,\n" +
                        "  order_id STRING,\n" +
                        "  refund_amount DECIMAL(20,2),\n" +
                        "  refund_status STRING,\n" +
                        "  refund_time TIMESTAMP(3),\n" +
                        "  WATERMARK FOR refund_time AS refund_time - INTERVAL '" + WATERMARK_DELAY_MIN + "' MINUTE\n" +
                        ") WITH (\n" +
                        "  'connector'='kafka',\n" +
                        "  'topic'='" + TOPIC_REFUND + "',\n" +
                        "  'properties.bootstrap.servers'='" + KAFKA_BOOTSTRAP + "',\n" +
                        "  'properties.group.id'='ads_refund',\n" +
                        "  'scan.startup.mode'='latest-offset',\n" +
                        "  'format'='json',\n" +
                        "  'json.ignore-parse-errors'='true'\n" +
                        ")");

        // 保留 cart 源，后续需要可扩展
        tEnv.executeSql(
                "CREATE TABLE cart_src (\n" +
                        "  shop_id STRING,\n" +
                        "  item_id STRING,\n" +
                        "  user_id STRING,\n" +
                        "  quantity BIGINT,\n" +
                        "  cart_time TIMESTAMP(3),\n" +
                        "  WATERMARK FOR cart_time AS cart_time - INTERVAL '" + WATERMARK_DELAY_MIN + "' MINUTE\n" +
                        ") WITH (\n" +
                        "  'connector'='kafka',\n" +
                        "  'topic'='" + TOPIC_CART + "',\n" +
                        "  'properties.bootstrap.servers'='" + KAFKA_BOOTSTRAP + "',\n" +
                        "  'properties.group.id'='ads_cart',\n" +
                        "  'scan.startup.mode'='latest-offset',\n" +
                        "  'format'='json',\n" +
                        "  'json.ignore-parse-errors'='true'\n" +
                        ")");

        // ---------- 日窗聚合（Table 视图） ----------
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW page_item_1d AS \n" +
                        "SELECT window_start, window_end, CAST(window_start AS DATE) AS dt,\n" +
                        "       shop_id, item_id,\n" +
                        "       COUNT(*) AS pv,\n" +
                        "       COUNT(DISTINCT user_id) AS uv\n" +
                        "FROM TABLE(TUMBLE(TABLE page_src, DESCRIPTOR(event_time), INTERVAL '" + WINDOW_SIZE_DAYS + "' DAY))\n" +
                        "GROUP BY window_start, window_end, shop_id, item_id");

        tEnv.executeSql(
                "CREATE TEMPORARY VIEW page_shop_1d AS \n" +
                        "SELECT window_start, window_end, CAST(window_start AS DATE) AS dt,\n" +
                        "       shop_id,\n" +
                        "       COUNT(*) AS pv,\n" +
                        "       COUNT(DISTINCT user_id) AS uv\n" +
                        "FROM TABLE(TUMBLE(TABLE page_src, DESCRIPTOR(event_time), INTERVAL '" + WINDOW_SIZE_DAYS + "' DAY))\n" +
                        "GROUP BY window_start, window_end, shop_id");

        tEnv.executeSql(
                "CREATE TEMPORARY VIEW order_item_1d AS \n" +
                        "SELECT window_start, window_end, CAST(window_start AS DATE) AS dt,\n" +
                        "       shop_id, item_id,\n" +
                        "       COUNT(DISTINCT buyer_id) AS order_buyer_cnt\n" +
                        "FROM TABLE(TUMBLE(TABLE order_src, DESCRIPTOR(order_time), INTERVAL '" + WINDOW_SIZE_DAYS + "' DAY))\n" +
                        "GROUP BY window_start, window_end, shop_id, item_id");

        tEnv.executeSql(
                "CREATE TEMPORARY VIEW pay_item_1d AS \n" +
                        "SELECT window_start, window_end, CAST(window_start AS DATE) AS dt,\n" +
                        "       shop_id, item_id,\n" +
                        "       COUNT(DISTINCT CASE WHEN pay_status='success' THEN buyer_id END) AS pay_buyer_cnt,\n" +
                        "       SUM(CASE WHEN pay_status='success' THEN pay_amount ELSE CAST(0 AS DECIMAL(20,2)) END) AS gross_pay_amt,\n" +
                        "       SUM(CASE WHEN pay_status='success' AND is_presale_tail_paid THEN 1 ELSE 0 END) AS presale_tail_pay_cnt,\n" +
                        "       SUM(CASE WHEN pay_status='success' AND is_zero_order_afterpay THEN 1 ELSE 0 END) AS afterpay_pay_cnt\n" +
                        "FROM TABLE(TUMBLE(TABLE pay_src, DESCRIPTOR(pay_time), INTERVAL '" + WINDOW_SIZE_DAYS + "' DAY))\n" +
                        "GROUP BY window_start, window_end, shop_id, item_id");

        tEnv.executeSql(
                "CREATE TEMPORARY VIEW refund_item_1d AS \n" +
                        "SELECT window_start, window_end, CAST(window_start AS DATE) AS dt,\n" +
                        "       shop_id, item_id,\n" +
                        "       SUM(CASE WHEN refund_status='success' THEN refund_amount ELSE CAST(0 AS DECIMAL(20,2)) END) AS refund_amt\n" +
                        "FROM TABLE(TUMBLE(TABLE refund_src, DESCRIPTOR(refund_time), INTERVAL '" + WINDOW_SIZE_DAYS + "' DAY))\n" +
                        "GROUP BY window_start, window_end, shop_id, item_id");

        tEnv.executeSql(
                "CREATE TEMPORARY VIEW combined_item_1d AS \n" +
                        "SELECT \n" +
                        "  COALESCE(pgi.dt, odi.dt, pyi.dt, rfi.dt) AS dt,\n" +
                        "  COALESCE(pgi.window_start, odi.window_start, pyi.window_start, rfi.window_start) AS window_start,\n" +
                        "  COALESCE(pgi.window_end,   odi.window_end,   pyi.window_end,   rfi.window_end)   AS window_end,\n" +
                        "  COALESCE(pgi.shop_id, odi.shop_id, pyi.shop_id, rfi.shop_id) AS shop_id,\n" +
                        "  COALESCE(pgi.item_id, odi.item_id, pyi.item_id, rfi.item_id) AS item_id,\n" +
                        "  COALESCE(pgi.pv, 0) AS pv,\n" +
                        "  COALESCE(pgi.uv, 0) AS uv,\n" +
                        "  COALESCE(odi.order_buyer_cnt, 0) AS order_buyer_cnt,\n" +
                        "  COALESCE(pyi.pay_buyer_cnt, 0) AS pay_buyer_cnt,\n" +
                        "  COALESCE(pyi.gross_pay_amt, CAST(0 AS DECIMAL(20,2))) AS gross_pay_amt,\n" +
                        "  COALESCE(rfi.refund_amt,    CAST(0 AS DECIMAL(20,2))) AS refund_amt,\n" +
                        "  GREATEST(\n" +
                        "    COALESCE(pyi.gross_pay_amt, CAST(0 AS DECIMAL(20,2))) - COALESCE(rfi.refund_amt, CAST(0 AS DECIMAL(20,2))),\n" +
                        "    CAST(0 AS DECIMAL(20,2))\n" +
                        "  ) AS net_pay_amt,\n" +
                        "  COALESCE(pyi.presale_tail_pay_cnt, 0) AS presale_tail_pay_cnt,\n" +
                        "  COALESCE(pyi.afterpay_pay_cnt, 0)     AS afterpay_pay_cnt\n" +
                        "FROM page_item_1d pgi\n" +
                        "FULL OUTER JOIN order_item_1d odi\n" +
                        "  ON pgi.shop_id=odi.shop_id AND pgi.item_id=odi.item_id AND pgi.window_start=odi.window_start AND pgi.window_end=odi.window_end\n" +
                        "FULL OUTER JOIN pay_item_1d pyi\n" +
                        "  ON COALESCE(pgi.shop_id,odi.shop_id)=pyi.shop_id AND COALESCE(pgi.item_id,odi.item_id)=pyi.item_id\n" +
                        " AND COALESCE(pgi.window_start,odi.window_start)=pyi.window_start AND COALESCE(pgi.window_end,odi.window_end)=pyi.window_end\n" +
                        "FULL OUTER JOIN refund_item_1d rfi\n" +
                        "  ON COALESCE(pgi.shop_id,odi.shop_id,pyi.shop_id)=rfi.shop_id AND COALESCE(pgi.item_id,odi.item_id,pyi.item_id)=rfi.item_id\n" +
                        " AND COALESCE(pgi.window_start,odi.window_start,pyi.window_start)=rfi.window_start AND COALESCE(pgi.window_end,odi.window_end,pyi.window_end)=rfi.window_end");

        // 最终结果表（只包含需要写入 CH 的列；顺序与 INSERT 占位符一致）
        Table itemTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "  dt, window_start, window_end, shop_id, item_id,\n" +
                        "  pv, uv, order_buyer_cnt, pay_buyer_cnt,\n" +
                        "  gross_pay_amt, refund_amt, net_pay_amt,\n" +
                        "  presale_tail_pay_cnt, afterpay_pay_cnt,\n" +
                        "  CURRENT_TIMESTAMP AS updated_at\n" +
                        "FROM combined_item_1d"
        );

        Table shopAggTable = tEnv.sqlQuery(
                "SELECT \n" +
                        "  c.dt, c.window_start, c.window_end, c.shop_id,\n" +
                        "  SUM(c.pv) AS pv,\n" +
                        "  s.uv      AS uv,\n" +
                        "  SUM(c.order_buyer_cnt) AS order_buyer_cnt,\n" +
                        "  SUM(c.pay_buyer_cnt)   AS pay_buyer_cnt,\n" +
                        "  SUM(c.gross_pay_amt)   AS gross_pay_amt,\n" +
                        "  SUM(c.refund_amt)      AS refund_amt,\n" +
                        "  SUM(c.net_pay_amt)     AS net_pay_amt,\n" +
                        "  CURRENT_TIMESTAMP      AS updated_at\n" +
                        "FROM combined_item_1d c\n" +
                        "LEFT JOIN page_shop_1d s\n" +
                        "  ON c.shop_id = s.shop_id\n" +
                        " AND c.window_start = s.window_start AND c.window_end = s.window_end\n" +
                        "GROUP BY c.dt, c.window_start, c.window_end, c.shop_id, s.uv"
        );

        // ---------- 转 DataStream（只保留 INSERT） ----------
        DataStream<Row> itemStream = tEnv.toChangelogStream(itemTable)
                .filter((FilterFunction<Row>) row -> row.getKind() == RowKind.INSERT)
                .name("item_result_stream");

        DataStream<Row> shopStream = tEnv.toChangelogStream(shopAggTable)
                .filter((FilterFunction<Row>) row -> row.getKind() == RowKind.INSERT)
                .name("shop_result_stream");

        // ---------- ClickHouse JdbcSink ----------
        String insertItemSql =
                "INSERT INTO ads_shop_item_daily (" +
                        " dt, window_start, window_end, shop_id, item_id," +
                        " pv, uv, order_buyer_cnt, pay_buyer_cnt," +
                        " gross_pay_amt, refund_amt, net_pay_amt," +
                        " presale_tail_pay_cnt, afterpay_pay_cnt, updated_at" +
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        String insertShopSql =
                "INSERT INTO ads_shop_daily (" +
                        " dt, window_start, window_end, shop_id," +
                        " pv, uv, order_buyer_cnt, pay_buyer_cnt," +
                        " gross_pay_amt, refund_amt, net_pay_amt, updated_at" +
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        JdbcExecutionOptions execOpts = JdbcExecutionOptions.builder()
                .withBatchSize(5000)
                .withBatchIntervalMs(1000)
                .withMaxRetries(3)
                .build();

        JdbcConnectionOptions connOpts = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(CK_URL + "?use_server_time_zone=1") // 建议带上时区对齐
                .withDriverName(CK_DRIVER)
                .withUsername(CK_USER)
                .withPassword(CK_PASSWORD)
                .build();

        // 写 ads_shop_item_daily
        itemStream.addSink(
                JdbcSink.sink(
                        insertItemSql,
                        (ps, r) -> {
                            // 下标从 1 开始，对齐上面的 SELECT 列顺序
                            // 0: dt (DATE)
                            LocalDate dt = (LocalDate) r.getField(0);
                            ps.setDate(1, Date.valueOf(dt));

                            // 1,2: window_start/end (TIMESTAMP)
                            LocalDateTime ws = (LocalDateTime) r.getField(1);
                            LocalDateTime we = (LocalDateTime) r.getField(2);
                            ps.setTimestamp(2, Timestamp.valueOf(ws));
                            ps.setTimestamp(3, Timestamp.valueOf(we));

                            // 3,4: shop_id, item_id
                            ps.setString(4, (String) r.getField(3));
                            ps.setString(5, (String) r.getField(4));

                            // 5..8: pv, uv, order_buyer_cnt, pay_buyer_cnt
                            ps.setLong(6,  (Long) r.getField(5));
                            ps.setLong(7,  (Long) r.getField(6));
                            ps.setLong(8,  (Long) r.getField(7));
                            ps.setLong(9,  (Long) r.getField(8));

                            // 9..11: gross/refund/net
                            ps.setBigDecimal(10, (java.math.BigDecimal) r.getField(9));
                            ps.setBigDecimal(11, (java.math.BigDecimal) r.getField(10));
                            ps.setBigDecimal(12, (java.math.BigDecimal) r.getField(11));

                            // 12..13: presale_tail_pay_cnt, afterpay_pay_cnt
                            ps.setLong(13, (Long) r.getField(12));
                            ps.setLong(14, (Long) r.getField(13));

                            // 14: updated_at
                            LocalDateTime upd = (LocalDateTime) r.getField(14);
                            ps.setTimestamp(15, Timestamp.valueOf(upd));
                        },
                        execOpts,
                        connOpts
                )
        ).name("ck_sink_ads_shop_item_daily");

        // 写 ads_shop_daily
        shopStream.addSink(
                JdbcSink.sink(
                        insertShopSql,
                        (ps, r) -> {
                            LocalDate dt = (LocalDate) r.getField(0);
                            ps.setDate(1, Date.valueOf(dt));

                            LocalDateTime ws = (LocalDateTime) r.getField(1);
                            LocalDateTime we = (LocalDateTime) r.getField(2);
                            ps.setTimestamp(2, Timestamp.valueOf(ws));
                            ps.setTimestamp(3, Timestamp.valueOf(we));

                            ps.setString(4, (String) r.getField(3)); // shop_id

                            ps.setLong(5,  (Long) r.getField(4));    // pv
                            ps.setLong(6,  (Long) r.getField(5));    // uv
                            ps.setLong(7,  (Long) r.getField(6));    // order_buyer_cnt
                            ps.setLong(8,  (Long) r.getField(7));    // pay_buyer_cnt
                            ps.setBigDecimal(9,  (java.math.BigDecimal) r.getField(8));  // gross
                            ps.setBigDecimal(10, (java.math.BigDecimal) r.getField(9));  // refund
                            ps.setBigDecimal(11, (java.math.BigDecimal) r.getField(10)); // net

                            LocalDateTime upd = (LocalDateTime) r.getField(11);
                            ps.setTimestamp(12, Timestamp.valueOf(upd));
                        },
                        execOpts,
                        connOpts
                )
        ).name("ck_sink_ads_shop_daily");

        env.execute("AdsShopMetricsToClickHouseJob (DataStream JdbcSink)");
    }
}
