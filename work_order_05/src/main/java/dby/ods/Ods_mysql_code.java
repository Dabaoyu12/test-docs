package dby.ods;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class Ods_mysql_code {

    // ====== 配置 ======
    private static final String MYSQL_HOST = "cdh01";
    private static final String MYSQL_PORT = "3306";
    private static final String MYSQL_DB = "work_order_05";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "123456";

    private static final String KAFKA_BOOTSTRAP_SERVERS = "cdh01:9092";
    private static final String KAFKA_TOPIC = "work_order_05";

    private static final long SLEEP_MS = 1L;
    private static final int BATCH_SIZE = 1000;

    private final Connection mysqlConn;
    private final Producer<String, String> kafkaProducer;
    private final Random random = new Random(12345);

    public Ods_mysql_code(Connection mysqlConn, Producer<String, String> kafkaProducer) {
        this.mysqlConn = mysqlConn;
        this.kafkaProducer = kafkaProducer;
    }

    public static void main(String[] args) throws Exception {
        String jdbcUrlAdmin = String.format("jdbc:mysql://%s:%s/?useSSL=false&serverTimezone=UTC",
                MYSQL_HOST, MYSQL_PORT);
        try (Connection adminConn = DriverManager.getConnection(jdbcUrlAdmin, MYSQL_USER, MYSQL_PASSWORD);
             Statement st = adminConn.createStatement()) {
            st.executeUpdate("CREATE DATABASE IF NOT EXISTS " + MYSQL_DB +
                    " CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci");
            System.out.println("确保数据库存在: " + MYSQL_DB);
        }

        String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&serverTimezone=UTC",
                MYSQL_HOST, MYSQL_PORT, MYSQL_DB);
        Connection mysqlConn = DriverManager.getConnection(jdbcUrl, MYSQL_USER, MYSQL_PASSWORD);
        mysqlConn.setAutoCommit(false);
        createTablesIfNotExist(mysqlConn);
        System.out.println("表结构确保完成");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        Producer<String, String> producer = new KafkaProducer<>(props);

        Ods_mysql_code simulator = new Ods_mysql_code(mysqlConn, producer);

        simulator.generateUsers(5000);
        simulator.generateShops(500);
        simulator.generateItems(5000);
        simulator.generateLogEvents(25000);
        simulator.generateOrders(5000);
        simulator.generateOrderItems(7500);
        simulator.generatePayments(1500);
        simulator.generateRefunds(500);

        producer.flush();
        producer.close();
        mysqlConn.close();
        System.out.println("模拟数据写入完成，总量约 50,000 条");
    }

    // ================= 建表 =================
    private static void createTablesIfNotExist(Connection conn) throws SQLException {
        String[] ddl = new String[]{
                "CREATE TABLE IF NOT EXISTS ods_user (" +
                        "user_id VARCHAR(50) PRIMARY KEY," +
                        "register_time TIMESTAMP," +
                        "gender VARCHAR(10)," +
                        "age INT," +
                        "region VARCHAR(100)," +
                        "device_type VARCHAR(50)," +
                        "update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",

                "CREATE TABLE IF NOT EXISTS ods_shop (" +
                        "shop_id VARCHAR(50) PRIMARY KEY," +
                        "shop_name VARCHAR(255)," +
                        "seller_id VARCHAR(50)," +
                        "category_id VARCHAR(50)," +
                        "create_time TIMESTAMP," +
                        "update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",

                "CREATE TABLE IF NOT EXISTS ods_item (" +
                        "item_id VARCHAR(50) PRIMARY KEY," +
                        "sku_id VARCHAR(50)," +
                        "shop_id VARCHAR(50)," +
                        "category_id VARCHAR(50)," +
                        "item_name VARCHAR(255)," +
                        "price DECIMAL(10,2)," +
                        "update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",

                "CREATE TABLE IF NOT EXISTS ods_log_events (" +
                        "id BIGINT AUTO_INCREMENT PRIMARY KEY," +
                        "event_id VARCHAR(50)," +
                        "event_time TIMESTAMP(3)," +
                        "event_type VARCHAR(50)," +
                        "user_id VARCHAR(50)," +
                        "device_id VARCHAR(100)," +
                        "session_id VARCHAR(100)," +
                        "item_id VARCHAR(50)," +
                        "shop_id VARCHAR(50)," +
                        "page_id VARCHAR(100)," +
                        "quantity INT," +
                        "props JSON," +
                        "create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",

                "CREATE TABLE IF NOT EXISTS ods_order (" +
                        "order_id VARCHAR(50) PRIMARY KEY," +
                        "buyer_id VARCHAR(50)," +
                        "order_amount DECIMAL(10,2)," +
                        "order_status VARCHAR(20)," +
                        "is_presale TINYINT(1)," +
                        "order_time TIMESTAMP," +
                        "create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",

                "CREATE TABLE IF NOT EXISTS ods_order_item (" +
                        "id BIGINT AUTO_INCREMENT PRIMARY KEY," +
                        "order_id VARCHAR(50)," +
                        "item_id VARCHAR(50)," +
                        "sku_id VARCHAR(50)," +
                        "shop_id VARCHAR(50)," +
                        "qty INT," +
                        "unit_price DECIMAL(10,2)," +
                        "create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",

                "CREATE TABLE IF NOT EXISTS ods_payment (" +
                        "payment_id VARCHAR(50) PRIMARY KEY," +
                        "order_id VARCHAR(50)," +
                        "buyer_id VARCHAR(50)," +
                        "pay_amount DECIMAL(10,2)," +
                        "pay_status VARCHAR(20)," +
                        "pay_time TIMESTAMP," +
                        "is_presale_deposit TINYINT(1)," +
                        "is_presale_tail_paid TINYINT(1)," +
                        "is_zero_order_afterpay TINYINT(1)," +
                        "refund_amount DECIMAL(10,2)," +
                        "create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",

                "CREATE TABLE IF NOT EXISTS ods_refund (" +
                        "refund_id VARCHAR(50) PRIMARY KEY," +
                        "order_id VARCHAR(50)," +
                        "buyer_id VARCHAR(50)," +
                        "refund_amount DECIMAL(10,2)," +
                        "refund_type VARCHAR(20)," +
                        "refund_time TIMESTAMP," +
                        "create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        };

        try (Statement st = conn.createStatement()) {
            for (String sql : ddl) {
                st.execute(sql);
            }
        }
    }

    // ================= Kafka Helper =================
    private void sendKafka(String key, Map<String, Object> map) {
        kafkaProducer.send(new ProducerRecord<>(KAFKA_TOPIC, key, JSON.toJSONString(map)));
    }

    // ================= 生成用户 =================
    public void generateUsers(int num) throws Exception {
        String sql = "INSERT INTO ods_user (user_id,register_time,gender,age,region,device_type) VALUES (?,?,?,?,?,?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            String[] genders = {"男", "女"};
            String[] regions = {"北京", "上海", "广州", "深圳", "杭州", "河北", "河南", "山西", "山东", "山西"};
            String[] devices = {"iOS", "Android", "PC"};
            for (int i = 0; i < num; i++) {
                String userId = "user_" + (i + 1);
                Timestamp regTime = new Timestamp(System.currentTimeMillis() - random.nextInt(1_000_000_000));
                String gender = genders[random.nextInt(genders.length)];
                int age = 18 + random.nextInt(50);
                String region = regions[random.nextInt(regions.length)];
                String device = devices[random.nextInt(devices.length)];

                ps.setString(1, userId);
                ps.setTimestamp(2, regTime);
                ps.setString(3, gender);
                ps.setInt(4, age);
                ps.setString(5, region);
                ps.setString(6, device);
                ps.addBatch();

                Map<String, Object> map = new HashMap<>();
                map.put("user_id", userId);
                map.put("register_time", regTime.toString());
                map.put("gender", gender);
                map.put("age", age);
                map.put("region", region);
                map.put("device_type", device);
                map.put("event_type", "user");
                sendKafka(userId, map);

                if (i % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    mysqlConn.commit();
                }
                Thread.sleep(SLEEP_MS);
            }
            ps.executeBatch();
            mysqlConn.commit();
        }
    }

    // ================= 生成店铺 =================
    public void generateShops(int num) throws Exception {
        String sql = "INSERT INTO ods_shop (shop_id,shop_name,seller_id,category_id,create_time) VALUES (?,?,?,?,?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            for (int i = 0; i < num; i++) {
                String shopId = "shop_" + (i + 1);
                Timestamp createTime = new Timestamp(System.currentTimeMillis() - random.nextInt(1_000_000_000));
                String shopName = "店铺_" + (i + 1);
                String sellerId = "seller_" + (random.nextInt(200) + 1);
                String categoryId = "cat_" + (random.nextInt(50) + 1);

                ps.setString(1, shopId);
                ps.setString(2, shopName);
                ps.setString(3, sellerId);
                ps.setString(4, categoryId);
                ps.setTimestamp(5, createTime);
                ps.addBatch();

                Map<String, Object> map = new HashMap<>();
                map.put("shop_id", shopId);
                map.put("shop_name", shopName);
                map.put("seller_id", sellerId);
                map.put("category_id", categoryId);
                map.put("create_time", createTime.toString());
                map.put("event_type", "shop");
                sendKafka(shopId, map);

                if (i % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    mysqlConn.commit();
                }
                Thread.sleep(SLEEP_MS);
            }
            ps.executeBatch();
            mysqlConn.commit();
        }
    }

    // ================= 生成商品 =================
    public void generateItems(int num) throws Exception {
        String sql = "INSERT INTO ods_item (item_id,sku_id,shop_id,category_id,item_name,price) VALUES (?,?,?,?,?,?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            for (int i = 0; i < num; i++) {
                String itemId = "item_" + (i + 1);
                String skuId = "sku_" + (i + 1);
                String shopId = "shop_" + (random.nextInt(500) + 1);
                String categoryId = "cat_" + (random.nextInt(50) + 1);
                String itemName = "商品_" + (i + 1);
                double price = Math.round((10 + random.nextDouble() * 1000) * 100.0) / 100.0;

                ps.setString(1, itemId);
                ps.setString(2, skuId);
                ps.setString(3, shopId);
                ps.setString(4, categoryId);
                ps.setString(5, itemName);
                ps.setDouble(6, price);
                ps.addBatch();

                Map<String, Object> map = new HashMap<>();
                map.put("item_id", itemId);
                map.put("sku_id", skuId);
                map.put("shop_id", shopId);
                map.put("category_id", categoryId);
                map.put("item_name", itemName);
                map.put("price", price);
                map.put("event_type", "item");
                sendKafka(itemId, map);

                if (i % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    mysqlConn.commit();
                }
                Thread.sleep(SLEEP_MS);
            }
            ps.executeBatch();
            mysqlConn.commit();
        }
    }

    // ================= 生成日志事件 =================
    public void generateLogEvents(int num) throws Exception {
        String sql = "INSERT INTO ods_log_events (event_id,event_time,event_type,user_id,device_id,session_id,item_id,shop_id,page_id,quantity,props) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            String[] types = {"start", "page_view", "display", "action", "error"};
            for (int i = 0; i < num; i++) {
                String eventId = "evt_" + (i + 1);
                String eventType = types[random.nextInt(types.length)];
                String userId = "user_" + (random.nextInt(5000) + 1);
                String itemId = "item_" + (random.nextInt(5000) + 1);
                String shopId = "shop_" + (random.nextInt(500) + 1);
                String sessionId = "sess_" + (random.nextInt(100000) + 1);
                int qty = eventType.equals("action") ? random.nextInt(3) + 1 : 1;
                Timestamp eventTime = new Timestamp(System.currentTimeMillis());
                String deviceId = "dev_" + (random.nextInt(10000) + 1);
                String pageId = "page_" + (random.nextInt(200) + 1);
                Map<String,Object> propsMap = new HashMap<>();
                propsMap.put("quantity", qty);
                String propsJson = JSON.toJSONString(propsMap);

                ps.setString(1, eventId);
                ps.setTimestamp(2, eventTime);
                ps.setString(3, eventType);
                ps.setString(4, userId);
                ps.setString(5, deviceId);
                ps.setString(6, sessionId);
                ps.setString(7, itemId);
                ps.setString(8, shopId);
                ps.setString(9, pageId);
                ps.setInt(10, qty);
                ps.setString(11, propsJson);
                ps.addBatch();

                // Kafka 分流
                String topic;
                switch (eventType) {
                    case "start": topic = "ods_start_log_05"; break;
                    case "page_view": topic = "ods_page_log_05"; break;
                    case "display": topic = "ods_display_log_05"; break;
                    case "action": topic = "ods_action_log_05"; break;
                    case "error": topic = "ods_error_log_05"; break;
                    default: topic = "work_order_05"; break;
                }

                Map<String,Object> map = new HashMap<>();
                map.put("event_id", eventId);
                map.put("event_time", eventTime.toString());
                map.put("event_type", eventType);
                map.put("user_id", userId);
                map.put("device_id", deviceId);
                map.put("session_id", sessionId);
                map.put("item_id", itemId);
                map.put("shop_id", shopId);
                map.put("page_id", pageId);
                map.put("quantity", qty);
                map.put("props", propsMap);

                kafkaProducer.send(new ProducerRecord<>(topic, eventId, JSON.toJSONString(map)));

                if (i % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    mysqlConn.commit();
                }
                Thread.sleep(SLEEP_MS);
            }
            ps.executeBatch();
            mysqlConn.commit();
        }
    }

    // ================= 生成订单 =================
    public void generateOrders(int num) throws Exception {
        String sql = "INSERT INTO ods_order (order_id,buyer_id,order_amount,order_status,is_presale,order_time) VALUES (?,?,?,?,?,?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            for (int i = 0; i < num; i++) {
                String orderId = "order_" + (i + 1); // 改为前缀+数字
                String buyerId = "user_" + (random.nextInt(5000) + 1);
                double amount = Math.round((20 + random.nextDouble() * 1000) * 100.0) / 100.0;
                boolean presale = random.nextDouble() < 0.05;
                Timestamp orderTime = new Timestamp(System.currentTimeMillis());

                ps.setString(1, orderId);
                ps.setString(2, buyerId);
                ps.setDouble(3, amount);
                ps.setString(4, "CREATED");
                ps.setInt(5, presale ? 1 : 0);
                ps.setTimestamp(6, orderTime);
                ps.addBatch();

                Map<String,Object> map = new HashMap<>();
                map.put("order_id", orderId);
                map.put("buyer_id", buyerId);
                map.put("order_amount", amount);
                map.put("order_status", "CREATED");
                map.put("is_presale", presale ? 1 : 0);
                map.put("order_time", orderTime.toString());
                map.put("event_type", "order");
                sendKafka(orderId, map);

                if (i % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    mysqlConn.commit();
                }
                Thread.sleep(SLEEP_MS);
            }
            ps.executeBatch();
            mysqlConn.commit();
        }
    }

    // ================= 生成订单明细 =================
    public void generateOrderItems(int num) throws Exception {
        String sql = "INSERT INTO ods_order_item (order_id,item_id,sku_id,shop_id,qty,unit_price) VALUES (?,?,?,?,?,?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            for (int i = 0; i < num; i++) {
                String orderId = "order_" + (i + 1);
                String itemId = "item_" + (random.nextInt(5000) + 1);
                String skuId = "sku_" + (random.nextInt(5000) + 1);
                String shopId = "shop_" + (random.nextInt(500) + 1);
                int qty = random.nextInt(3) + 1;
                double price = Math.round((5 + random.nextDouble() * 1000) * 100.0) / 100.0;

                ps.setString(1, orderId);
                ps.setString(2, itemId);
                ps.setString(3, skuId);
                ps.setString(4, shopId);
                ps.setInt(5, qty);
                ps.setDouble(6, price);
                ps.addBatch();

                Map<String,Object> map = new HashMap<>();
                map.put("order_id", orderId);
                map.put("item_id", itemId);
                map.put("sku_id", skuId);
                map.put("shop_id", shopId);
                map.put("qty", qty);
                map.put("unit_price", price);
                map.put("event_type", "order_item");
                sendKafka(orderId, map);

                if (i % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    mysqlConn.commit();
                }
                Thread.sleep(SLEEP_MS);
            }
            ps.executeBatch();
            mysqlConn.commit();
        }
    }

    // ================= 生成支付 =================
    public void generatePayments(int num) throws Exception {
        String sql = "INSERT INTO ods_payment (payment_id,order_id,buyer_id,pay_amount,pay_status,pay_time,is_presale_deposit,is_presale_tail_paid,is_zero_order_afterpay,refund_amount) VALUES (?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            for (int i = 0; i < num; i++) {
                String paymentId = "pay_" + (i + 1);
                String orderId = "order_" + (i + 1);
                String buyerId = "user_" + (random.nextInt(5000) + 1);
                double payAmt = Math.round((5 + random.nextDouble() * 2000) * 100.0) / 100.0;
                boolean deposit = random.nextDouble() < 0.1;
                boolean tail = !deposit && random.nextDouble() < 0.5;
                boolean zeroAfterpay = random.nextDouble() < 0.02;
                double refund = random.nextDouble() < 0.05 ? Math.round(random.nextDouble() * payAmt * 100.0) / 100.0 : 0.0;
                Timestamp payTime = new Timestamp(System.currentTimeMillis());

                ps.setString(1, paymentId);
                ps.setString(2, orderId);
                ps.setString(3, buyerId);
                ps.setDouble(4, payAmt);
                ps.setString(5, "SUCCESS");
                ps.setTimestamp(6, payTime);
                ps.setInt(7, deposit ? 1 : 0);
                ps.setInt(8, tail ? 1 : 0);
                ps.setInt(9, zeroAfterpay ? 1 : 0);
                ps.setDouble(10, refund);
                ps.addBatch();

                Map<String,Object> map = new HashMap<>();
                map.put("payment_id", paymentId);
                map.put("order_id", orderId);
                map.put("buyer_id", buyerId);
                map.put("pay_amount", payAmt);
                map.put("pay_status", "SUCCESS");
                map.put("pay_time", payTime.toString());
                map.put("is_presale_deposit", deposit ? 1 : 0);
                map.put("is_presale_tail_paid", tail ? 1 : 0);
                map.put("is_zero_order_afterpay", zeroAfterpay ? 1 : 0);
                map.put("refund_amount", refund);
                map.put("event_type", "payment");
                sendKafka(paymentId, map);

                if (i % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    mysqlConn.commit();
                }
                Thread.sleep(SLEEP_MS);
            }
            ps.executeBatch();
            mysqlConn.commit();
        }
    }

    // ================= 生成退款 =================
    public void generateRefunds(int num) throws Exception {
        String sql = "INSERT INTO ods_refund (refund_id,order_id,buyer_id,refund_amount,refund_type,refund_time) VALUES (?,?,?,?,?,?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            String[] types = {"售中退款", "售后退款", "秒退"};
            for (int i = 0; i < num; i++) {
                String refundId = "refund_" + (i + 1);
                String orderId = "order_" + (i + 1);
                String buyerId = "user_" + (random.nextInt(5000) + 1);
                double amt = Math.round(random.nextDouble() * 500 * 100.0) / 100.0;
                String type = types[random.nextInt(types.length)];
                Timestamp refundTime = new Timestamp(System.currentTimeMillis());

                ps.setString(1, refundId);
                ps.setString(2, orderId);
                ps.setString(3, buyerId);
                ps.setDouble(4, amt);
                ps.setString(5, type);
                ps.setTimestamp(6, refundTime);
                ps.addBatch();

                Map<String,Object> map = new HashMap<>();
                map.put("refund_id", refundId);
                map.put("order_id", orderId);
                map.put("buyer_id", buyerId);
                map.put("refund_amount", amt);
                map.put("refund_type", type);
                map.put("refund_time", refundTime.toString());
                map.put("event_type", "refund");
                sendKafka(refundId, map);

                if (i % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    mysqlConn.commit();
                }
                Thread.sleep(SLEEP_MS);
            }
            ps.executeBatch();
            mysqlConn.commit();
        }
    }
}
