package dby.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Dwd_to_kfk {
    public static void main(String[] args) throws Exception {
        // 初始化 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String kafkaServers = "cdh01:9092";

        // 1. Source：work_order_05 源数据
        tableEnv.executeSql(
                "CREATE TABLE work_order_05 ( " +
                        " shop_id STRING, " +
                        " event_type STRING, " +
                        " order_id STRING, " +
                        " item_id STRING, " +
                        " sku_id STRING, " +
                        " qty INT, " +
                        " unit_price DECIMAL(10,2), " +
                        " payment_id STRING, " +
                        " pay_amount DECIMAL(10,2), " +
                        " pay_status STRING, " +
                        " pay_time STRING, " +
                        " buyer_id STRING, " +
                        " refund_id STRING, " +
                        " refund_amount DECIMAL(10,2), " +
                        " refund_type STRING, " +
                        " user_id STRING, " +
                        " register_time STRING, " +
                        " gender STRING, " +
                        " age INT, " +
                        " region STRING, " +
                        " device_type STRING, " +
                        " item_name STRING, " +
                        " category_id STRING, " +
                        " price DECIMAL(10,2), " +
                        " shop_name STRING, " +
                        " seller_id STRING, " +
                        " create_time STRING " +
                        ") WITH ( " +
                        " 'connector' = 'kafka', " +
                        " 'topic' = 'work_order_05', " +
                        " 'properties.bootstrap.servers' = '" + kafkaServers + "', " +
                        " 'scan.startup.mode' = 'earliest-offset', " +
                        " 'format' = 'json' " +
                        ")"
        );

        // 2. Sink：订单明细
        tableEnv.executeSql(
                "CREATE TABLE dwd_order_item_05 ( " +
                        " order_id STRING, item_id STRING, sku_id STRING, shop_id STRING, " +
                        " qty INT, unit_price DECIMAL(10,2) " +
                        ") WITH ( " +
                        " 'connector' = 'kafka', " +
                        " 'topic' = 'dwd_order_item_05', " +
                        " 'properties.bootstrap.servers' = '" + kafkaServers + "', " +
                        " 'format' = 'json' " +
                        ")"
        );

        // 3. Sink：支付
        tableEnv.executeSql(
                "CREATE TABLE dwd_payment_05 ( " +
                        " payment_id STRING, order_id STRING, buyer_id STRING, " +
                        " pay_amount DECIMAL(10,2), pay_status STRING, pay_time STRING " +
                        ") WITH ( " +
                        " 'connector' = 'kafka', " +
                        " 'topic' = 'dwd_payment_05', " +
                        " 'properties.bootstrap.servers' = '" + kafkaServers + "', " +
                        " 'format' = 'json' " +
                        ")"
        );

        // 4. Sink：退款
        tableEnv.executeSql(
                "CREATE TABLE dwd_refund_05 ( " +
                        " refund_id STRING, order_id STRING, buyer_id STRING, " +
                        " refund_amount DECIMAL(10,2), refund_type STRING " +
                        ") WITH ( " +
                        " 'connector' = 'kafka', " +
                        " 'topic' = 'dwd_refund_05', " +
                        " 'properties.bootstrap.servers' = '" + kafkaServers + "', " +
                        " 'format' = 'json' " +
                        ")"
        );

        // 5. Sink：用户
        tableEnv.executeSql(
                "CREATE TABLE dwd_user_05 ( " +
                        " user_id STRING, register_time STRING, gender STRING, age INT, " +
                        " region STRING, device_type STRING " +
                        ") WITH ( " +
                        " 'connector' = 'kafka', " +
                        " 'topic' = 'dwd_user_05', " +
                        " 'properties.bootstrap.servers' = '" + kafkaServers + "', " +
                        " 'format' = 'json' " +
                        ")"
        );

        // 6. Sink：商品
        tableEnv.executeSql(
                "CREATE TABLE dwd_item_05 ( " +
                        " item_id STRING, sku_id STRING, shop_id STRING, category_id STRING, " +
                        " item_name STRING, price DECIMAL(10,2) " +
                        ") WITH ( " +
                        " 'connector' = 'kafka', " +
                        " 'topic' = 'dwd_item_05', " +
                        " 'properties.bootstrap.servers' = '" + kafkaServers + "', " +
                        " 'format' = 'json' " +
                        ")"
        );

        // 7. Sink：店铺
        tableEnv.executeSql(
                "CREATE TABLE dwd_shop_05 ( " +
                        " shop_id STRING, shop_name STRING, seller_id STRING, category_id STRING, create_time STRING " +
                        ") WITH ( " +
                        " 'connector' = 'kafka', " +
                        " 'topic' = 'dwd_shop_05', " +
                        " 'properties.bootstrap.servers' = '" + kafkaServers + "', " +
                        " 'format' = 'json' " +
                        ")"
        );

        // ======== 分流逻辑 ========

        // 订单明细
        tableEnv.executeSql(
                "INSERT INTO dwd_order_item_05 " +
                        "SELECT order_id, item_id, sku_id, shop_id, qty, unit_price " +
                        "FROM work_order_05 WHERE event_type = 'order_item'"
        );

        // 支付
        tableEnv.executeSql(
                "INSERT INTO dwd_payment_05 " +
                        "SELECT payment_id, order_id, buyer_id, pay_amount, pay_status, pay_time " +
                        "FROM work_order_05 WHERE event_type = 'payment'"
        );

        // 退款
        tableEnv.executeSql(
                "INSERT INTO dwd_refund_05 " +
                        "SELECT refund_id, order_id, buyer_id, refund_amount, refund_type " +
                        "FROM work_order_05 WHERE event_type = 'refund'"
        );

        // 用户
        tableEnv.executeSql(
                "INSERT INTO dwd_user_05 " +
                        "SELECT user_id, register_time, gender, age, region, device_type " +
                        "FROM work_order_05 WHERE event_type = 'user'"
        );

        // 商品
        tableEnv.executeSql(
                "INSERT INTO dwd_item_05 " +
                        "SELECT item_id, sku_id, shop_id, category_id, item_name, price " +
                        "FROM work_order_05 WHERE event_type = 'item'"
        );

        // 店铺
        tableEnv.executeSql(
                "INSERT INTO dwd_shop_05 " +
                        "SELECT shop_id, shop_name, seller_id, category_id, create_time " +
                        "FROM work_order_05 WHERE event_type = 'shop'"
        );

        env.execute("ODS -> DWD Business Cleansing Job (event_type split)");
    }
}
