package dby.check;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

/**
 * 兼容你的 pom（ru.yandex.clickhouse:clickhouse-jdbc:0.3.2）
 * ClickHouse: cdh01  用户: root  密码: 123456  库: work_order_05
 *
 * 已存在表：
 *   - work_order_05.ads_shop_item_daily
 *   - work_order_05.ads_shop_daily
 * 本程序会自动创建：
 *   - work_order_05.ads_shop_item_cart_daily  （用于“加购件数 Top50”）
 *
 * 数据范围：2025-08-01 ~ 2025-08-07 （可重跑，先 DROP PARTITION 再写入）
 */
public class SimulateToClickHouse {

    // ===== ClickHouse 连接配置 =====
    private static final String DB       = "work_order_05";
    private static final String HOST     = "cdh01";
    private static final int    PORT     = 8123; // 若你用 19000 就改这里
    private static final String USER     = "root";
    private static final String PASSWORD = "123456";
    private static final String JDBC_URL = String.format("jdbc:clickhouse://%s:%d/%s", HOST, PORT, DB);

    // ===== 数据规模 =====
    private static final int NUM_SHOPS      = 60; // S0001..S0060
    private static final int ITEMS_PER_SHOP = 80; // 每店 80 个商品

    // ===== 日期范围 =====
    private static final LocalDate START_DATE = LocalDate.of(2025, 8, 1);
    private static final LocalDate END_DATE   = LocalDate.of(2025, 8, 7);

    // ===== 价格桶 =====
    private static final BigDecimal[][] PRICE_BUCKETS = new BigDecimal[][]{
            {bd("19.9"),  bd("59.9")},
            {bd("60"),    bd("199")},
            {bd("200"),   bd("599")},
            {bd("600"),   bd("1499")}
    };

    private static final int BATCH_SIZE = 10_000;

    public static void main(String[] args) throws Exception {
        // 使用与 pom 匹配的老驱动（支持 CH 20.5）
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");

        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        // 老驱动常用的稳态参数（可选）
        props.setProperty("socket_timeout", "600000");
        // 如果你有时区问题，可加：props.setProperty("use_server_time_zone", "true");

        try (Connection conn = DriverManager.getConnection(JDBC_URL, props);
             Statement stmt = conn.createStatement()) {

            // 1) 建（若无）加购表
            createCartTableIfNotExists(stmt);

            // 2) 清理 7 天分区（避免重复）
            for (LocalDate d = START_DATE; !d.isAfter(END_DATE); d = d.plusDays(1)) {
                String ds = d.toString();
                dropPartitionSafe(stmt, "ads_shop_item_daily", ds);
                dropPartitionSafe(stmt, "ads_shop_daily", ds);
                dropPartitionSafe(stmt, "ads_shop_item_cart_daily", ds);
            }

            // 3) 生成 shop / item
            List<String> shops = new ArrayList<>(NUM_SHOPS);
            for (int i = 1; i <= NUM_SHOPS; i++) shops.add(String.format("S%04d", i));

            Map<String, List<String>> itemsByShop = new HashMap<>();
            for (String s : shops) {
                List<String> items = new ArrayList<>(ITEMS_PER_SHOP);
                for (int j = 1; j <= ITEMS_PER_SHOP; j++) {
                    items.add(String.format("I%s_%04d", s.substring(1), j)); // I0001_0001
                }
                itemsByShop.put(s, items);
            }

            // 4) 写入 item_daily & cart_daily（无事务）
            final String insertItemSql =
                    "INSERT INTO " + DB + ".ads_shop_item_daily ("
                            + " dt, window_start, window_end, shop_id, item_id,"
                            + " pv, uv, order_buyer_cnt, pay_buyer_cnt,"
                            + " gross_pay_amt, refund_amt, net_pay_amt,"
                            + " presale_tail_pay_cnt, afterpay_pay_cnt"
                            + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            final String insertCartSql =
                    "INSERT INTO " + DB + ".ads_shop_item_cart_daily ("
                            + " dt, shop_id, item_id, cart_cnt"
                            + ") VALUES (?,?,?,?)";

            try (PreparedStatement psItem = conn.prepareStatement(insertItemSql);
                 PreparedStatement psCart = conn.prepareStatement(insertCartSql)) {

                for (LocalDate day = START_DATE; !day.isAfter(END_DATE); day = day.plusDays(1)) {
                    int cntItem = 0, cntCart = 0;

                    for (String shop : shops) {
                        for (String item : itemsByShop.get(shop)) {
                            ItemMetrics m = genItemMetrics(day, shop, item);

                            // item_daily
                            psItem.setDate(1, Date.valueOf(day));
                            psItem.setTimestamp(2, Timestamp.valueOf(m.windowStart));
                            psItem.setTimestamp(3, Timestamp.valueOf(m.windowEnd));
                            psItem.setString(4, shop);
                            psItem.setString(5, item);
                            psItem.setLong(6,  m.pv);
                            psItem.setLong(7,  m.uv);
                            psItem.setLong(8,  m.orderBuyerCnt);
                            psItem.setLong(9,  m.payBuyerCnt);
                            psItem.setBigDecimal(10, m.grossPayAmt);
                            psItem.setBigDecimal(11, m.refundAmt);
                            psItem.setBigDecimal(12, m.netPayAmt);
                            psItem.setLong(13, m.presaleTailPayCnt);
                            psItem.setLong(14, m.afterpayPayCnt);
                            psItem.addBatch();
                            cntItem++;

                            // cart_daily
                            psCart.setDate(1, Date.valueOf(day));
                            psCart.setString(2, shop);
                            psCart.setString(3, item);
                            psCart.setLong(4, m.cartCnt);
                            psCart.addBatch();
                            cntCart++;

                            if (cntItem % BATCH_SIZE == 0) {
                                psItem.executeBatch();
                                psCart.executeBatch();
                            }
                        }
                    }
                    // flush 当天剩余
                    psItem.executeBatch();
                    psCart.executeBatch();

                    // 由 item 聚合写入 shop_daily（只聚当天）
                    String insertShopDaily =
                            "INSERT INTO " + DB + ".ads_shop_daily ("
                                    + " dt, window_start, window_end, shop_id,"
                                    + " pv, uv, order_buyer_cnt, pay_buyer_cnt,"
                                    + " gross_pay_amt, refund_amt, net_pay_amt"
                                    + ") "
                                    + "SELECT dt, min(window_start), max(window_end), shop_id, "
                                    + "       sum(pv), sum(uv), sum(order_buyer_cnt), sum(pay_buyer_cnt), "
                                    + "       sum(gross_pay_amt), sum(refund_amt), sum(net_pay_amt) "
                                    + "FROM " + DB + ".ads_shop_item_daily "
                                    + "WHERE dt = toDate('" + day + "') "
                                    + "GROUP BY dt, shop_id";
                    stmt.execute(insertShopDaily);

                    System.out.println("Done day: " + day + "  items=" + cntItem + " carts=" + cntCart);
                }
            }

            System.out.println("✅ Mock data generated successfully for 2025-08-01 ~ 2025-08-07.");
        }
    }

    // ================= 工具 & 业务逻辑 =================

    private static void createCartTableIfNotExists(Statement stmt) throws SQLException {
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + DB + ".ads_shop_item_cart_daily ("
                        + " dt Date,"
                        + " shop_id LowCardinality(String),"
                        + " item_id LowCardinality(String),"
                        + " cart_cnt UInt64,"
                        + " updated_at DateTime DEFAULT now()"
                        + ") ENGINE = ReplacingMergeTree(updated_at)"
                        + " PARTITION BY dt"
                        + " ORDER BY (dt, shop_id, item_id)";
        stmt.execute(ddl);
    }

    private static void dropPartitionSafe(Statement stmt, String table, String partition) throws SQLException {
        try {
            stmt.execute("ALTER TABLE " + DB + "." + table + " DROP PARTITION '" + partition + "'");
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            if (!(msg.contains("unknown partition") || msg.contains("no partition"))) {
                // 其它异常抛出
                throw e;
            }
        }
    }

    private static BigDecimal bd(String s) {
        return new BigDecimal(s).setScale(2, RoundingMode.HALF_UP);
    }

    private static BigDecimal bd(double d) {
        return BigDecimal.valueOf(d).setScale(2, RoundingMode.HALF_UP);
    }

    private static long clampLong(long v, long lo, long hi) {
        return Math.max(lo, Math.min(hi, v));
    }

    private static int clampInt(int v, int lo, int hi) {
        return Math.max(lo, Math.min(hi, v));
    }

    /** 确定性种子：day|shop|item */
    private static long stableSeed(LocalDate day, String shopId, String itemId) {
        String key = day + "|" + shopId + "|" + itemId;
        return (long) Math.abs(key.hashCode());
    }

    /** 简单三角分布 */
    private static double triangular(Random r, double a, double b, double mode) {
        double u = r.nextDouble();
        double c = (mode - a) / (b - a);
        if (u < c) {
            return a + Math.sqrt(u * (b - a) * (mode - a));
        } else {
            return b - Math.sqrt((1 - u) * (b - a) * (b - mode));
        }
    }

    /** 业务口径模拟，满足：pv>=uv，order<=pay<=uv，net>=0，预售/后付之和<=pay */
    private static ItemMetrics genItemMetrics(LocalDate day, String shopId, String itemId) {
        Random rand = new Random(stableSeed(day, shopId, itemId));

        // UV：80~8000（长尾）
        int uv = (int) Math.round(triangular(rand, 80, 8000, 600));
        uv = Math.max(1, uv);

        // PV：1.2x ~ 6x
        long pv = Math.round(uv * (1.2 + rand.nextDouble() * (6.0 - 1.2)));
        pv = clampLong(pv, uv, uv * 10L);

        // 下单买家：UV 的 5%~25%
        double orderRate = 0.05 + rand.nextDouble() * (0.25 - 0.05);
        long orderBuyerCnt = clampLong(Math.round(uv * orderRate), 0, uv);

        // 支付买家：订单的 75%~98%
        double payRate = 0.75 + rand.nextDouble() * (0.98 - 0.75);
        long payBuyerCnt = clampLong(Math.round(orderBuyerCnt * payRate), 0, orderBuyerCnt);

        // 价格桶 + 桶内价格
        BigDecimal[] bucket = PRICE_BUCKETS[rand.nextInt(PRICE_BUCKETS.length)];
        BigDecimal pMin = bucket[0], pMax = bucket[1];
        double price = pMin.doubleValue() + rand.nextDouble() * (pMax.doubleValue() - pMin.doubleValue());
        BigDecimal avgPrice = bd(price);

        // 每买家件数（倾向 1.2）
        int qtyPerBuyer = clampInt((int) Math.round(triangular(rand, 1, 3, 1.2)), 1, 3);

        BigDecimal grossPayAmt = avgPrice
                .multiply(BigDecimal.valueOf(qtyPerBuyer))
                .multiply(BigDecimal.valueOf(payBuyerCnt))
                .setScale(2, RoundingMode.HALF_UP);

        // 退款 0%~18%
        double refundRate = rand.nextDouble() * 0.18;
        BigDecimal refundAmt = grossPayAmt.multiply(BigDecimal.valueOf(refundRate))
                .setScale(2, RoundingMode.HALF_UP);
        if (refundAmt.compareTo(grossPayAmt) > 0) refundAmt = grossPayAmt;

        BigDecimal netPayAmt = grossPayAmt.subtract(refundAmt)
                .max(BigDecimal.ZERO).setScale(2, RoundingMode.HALF_UP);

        // 预售尾款 / 先用后付
        long presaleTail = 0, afterPay = 0;
        if (payBuyerCnt > 0) {
            presaleTail = (long) Math.floor(payBuyerCnt * (rand.nextDouble() * 0.30));
            long remain = Math.max(0, payBuyerCnt - presaleTail);
            afterPay = Math.min(remain, (long) Math.floor(payBuyerCnt * (rand.nextDouble() * 0.25)));
        }

        // 加购件数：UV 的 20%~90%，mode=35%
        long cartCnt = Math.round(uv * triangular(rand, 0.20, 0.90, 0.35));

        LocalDateTime ws = LocalDateTime.of(day, LocalTime.MIN);
        LocalDateTime we = LocalDateTime.of(day, LocalTime.of(23, 59, 59));

        ItemMetrics m = new ItemMetrics();
        m.windowStart = ws;
        m.windowEnd = we;
        m.pv = pv;
        m.uv = uv;
        m.orderBuyerCnt = orderBuyerCnt;
        m.payBuyerCnt = payBuyerCnt;
        m.grossPayAmt = grossPayAmt;
        m.refundAmt = refundAmt;
        m.netPayAmt = netPayAmt;
        m.presaleTailPayCnt = presaleTail;
        m.afterpayPayCnt = afterPay;
        m.cartCnt = Math.max(0, cartCnt);
        return m;
    }

    private static class ItemMetrics {
        LocalDateTime windowStart;
        LocalDateTime windowEnd;
        long pv;
        long uv;
        long orderBuyerCnt;
        long payBuyerCnt;
        BigDecimal grossPayAmt;
        BigDecimal refundAmt;
        BigDecimal netPayAmt;
        long presaleTailPayCnt;
        long afterpayPayCnt;
        long cartCnt;
    }
}
