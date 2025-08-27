package dby.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.HashMap;
import java.util.Map;

/**
 * Flink 链表式处理 work_order_05 数据并写入 DWD Kafka 表（全部表）
 */
public class DwdLinkedListJob {

    // 链表节点
    static class EventNode {
        String eventType;
        Map<String, Object> data;
        EventNode next;

        EventNode(String eventType, Map<String, Object> data) {
            this.eventType = eventType;
            this.data = data;
        }
    }

    // 链表管理
    static class EventLinkedList {
        EventNode head;
        EventNode tail;

        void addEvent(String eventType, Map<String, Object> data) {
            EventNode node = new EventNode(eventType, data);
            if (head == null) head = tail = node;
            else { tail.next = node; tail = node; }
        }

        void processEvents(StreamTableEnvironment tableEnv) {
            EventNode curr = head;
            while (curr != null) {
                String insertSql = "";

                switch (curr.eventType) {
                    case "order":
                        insertSql = String.format(
                                "INSERT INTO dwd_order_05 (order_id,buyer_id,order_amount,order_status,is_presale,order_time,ts) " +
                                        "VALUES ('%s','%s',%s,'%s',%s,'%s',%s)",
                                curr.data.get("order_id"),
                                curr.data.get("buyer_id"),
                                curr.data.get("order_amount"),
                                curr.data.get("order_status"),
                                curr.data.get("is_presale"),
                                curr.data.get("order_time"),
                                curr.data.get("ts")
                        );
                        break;
                    case "order_item":
                        insertSql = String.format(
                                "INSERT INTO dwd_order_item_05 (id,order_id,item_id,sku_id,shop_id,qty,unit_price,ts) " +
                                        "VALUES ('%s','%s','%s','%s','%s',%s,%s,%s)",
                                curr.data.get("id"),
                                curr.data.get("order_id"),
                                curr.data.get("item_id"),
                                curr.data.get("sku_id"),
                                curr.data.get("shop_id"),
                                curr.data.get("qty"),
                                curr.data.get("unit_price"),
                                curr.data.get("ts")
                        );
                        break;
                    case "payment":
                        insertSql = String.format(
                                "INSERT INTO dwd_payment_05 (payment_id,order_id,buyer_id,pay_amount,pay_status,pay_time,ts) " +
                                        "VALUES ('%s','%s','%s',%s,'%s','%s',%s)",
                                curr.data.get("payment_id"),
                                curr.data.get("order_id"),
                                curr.data.get("buyer_id"),
                                curr.data.get("pay_amount"),
                                curr.data.get("pay_status"),
                                curr.data.get("pay_time"),
                                curr.data.get("ts")
                        );
                        break;
                    case "refund":
                        insertSql = String.format(
                                "INSERT INTO dwd_refund_05 (refund_id,order_id,buyer_id,refund_amount,refund_type,refund_time,ts) " +
                                        "VALUES ('%s','%s','%s',%s,'%s','%s',%s)",
                                curr.data.get("refund_id"),
                                curr.data.get("order_id"),
                                curr.data.get("buyer_id"),
                                curr.data.get("refund_amount"),
                                curr.data.get("refund_type"),
                                curr.data.get("refund_time"),
                                curr.data.get("ts")
                        );
                        break;
                    case "user":
                        insertSql = String.format(
                                "INSERT INTO dwd_user_05 (user_id,register_time,gender,age,region,device_type,ts) " +
                                        "VALUES ('%s','%s','%s',%s,'%s','%s',%s)",
                                curr.data.get("user_id"),
                                curr.data.get("register_time"),
                                curr.data.get("gender"),
                                curr.data.get("age"),
                                curr.data.get("region"),
                                curr.data.get("device_type"),
                                curr.data.get("ts")
                        );
                        break;
                    case "item":
                        insertSql = String.format(
                                "INSERT INTO dwd_item_05 (item_id,sku_id,shop_id,category_id,item_name,price,ts) " +
                                        "VALUES ('%s','%s','%s','%s','%s',%s,%s)",
                                curr.data.get("item_id"),
                                curr.data.get("sku_id"),
                                curr.data.get("shop_id"),
                                curr.data.get("category_id"),
                                curr.data.get("item_name"),
                                curr.data.get("price"),
                                curr.data.get("ts")
                        );
                        break;
                    case "shop":
                        insertSql = String.format(
                                "INSERT INTO dwd_shop_05 (shop_id,shop_name,seller_id,category_id,create_time,ts) " +
                                        "VALUES ('%s','%s','%s','%s','%s',%s)",
                                curr.data.get("shop_id"),
                                curr.data.get("shop_name"),
                                curr.data.get("seller_id"),
                                curr.data.get("category_id"),
                                curr.data.get("create_time"),
                                curr.data.get("ts")
                        );
                        break;
                }

                if (!insertSql.isEmpty()) {
                    TableResult result = tableEnv.executeSql(insertSql);
                    result.print(); // 打印执行结果，调试用
                }

                curr = curr.next;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String kafkaServers = "cdh01:9092";

        // ===== 定义全部 DWD Kafka 表 =====
        String[] createTableSQLs = new String[]{
                "CREATE TABLE dwd_order_05 (order_id STRING, buyer_id STRING, order_amount DECIMAL(10,2), order_status STRING, is_presale INT, order_time STRING, ts BIGINT) WITH ('connector'='kafka','topic'='dwd_order_05','properties.bootstrap.servers'='" + kafkaServers + "','format'='json')",
                "CREATE TABLE dwd_order_item_05 (id STRING, order_id STRING, item_id STRING, sku_id STRING, shop_id STRING, qty INT, unit_price DECIMAL(10,2), ts BIGINT) WITH ('connector'='kafka','topic'='dwd_order_item_05','properties.bootstrap.servers'='" + kafkaServers + "','format'='json')",
                "CREATE TABLE dwd_payment_05 (payment_id STRING, order_id STRING, buyer_id STRING, pay_amount DECIMAL(10,2), pay_status STRING, pay_time STRING, ts BIGINT) WITH ('connector'='kafka','topic'='dwd_payment_05','properties.bootstrap.servers'='" + kafkaServers + "','format'='json')",
                "CREATE TABLE dwd_refund_05 (refund_id STRING, order_id STRING, buyer_id STRING, refund_amount DECIMAL(10,2), refund_type STRING, refund_time STRING, ts BIGINT) WITH ('connector'='kafka','topic'='dwd_refund_05','properties.bootstrap.servers'='" + kafkaServers + "','format'='json')",
                "CREATE TABLE dwd_user_05 (user_id STRING, register_time STRING, gender STRING, age INT, region STRING, device_type STRING, ts BIGINT) WITH ('connector'='kafka','topic'='dwd_user_05','properties.bootstrap.servers'='" + kafkaServers + "','format'='json')",
                "CREATE TABLE dwd_item_05 (item_id STRING, sku_id STRING, shop_id STRING, category_id STRING, item_name STRING, price DECIMAL(10,2), ts BIGINT) WITH ('connector'='kafka','topic'='dwd_item_05','properties.bootstrap.servers'='" + kafkaServers + "','format'='json')",
                "CREATE TABLE dwd_shop_05 (shop_id STRING, shop_name STRING, seller_id STRING, category_id STRING, create_time STRING, ts BIGINT) WITH ('connector'='kafka','topic'='dwd_shop_05','properties.bootstrap.servers'='" + kafkaServers + "','format'='json')"
        };

        for (String sql : createTableSQLs) tableEnv.executeSql(sql);

        // ===== 模拟读取 work_order_05 数据加入链表 =====
        EventLinkedList linkedList = new EventLinkedList();

        // 示例 order_item
        Map<String,Object> orderItem = new HashMap<>();
        orderItem.put("id","1"); orderItem.put("order_id","order_7498"); orderItem.put("item_id","item_831");
        orderItem.put("sku_id","sku_851"); orderItem.put("shop_id","shop_7"); orderItem.put("qty",2);
        orderItem.put("unit_price",11.06); orderItem.put("ts",System.currentTimeMillis());
        linkedList.addEvent("order_item", orderItem);

        // 示例 payment
        Map<String,Object> payment = new HashMap<>();
        payment.put("payment_id","pay_1"); payment.put("order_id","order_7498"); payment.put("buyer_id","user_123");
        payment.put("pay_amount",1397.33); payment.put("pay_status","SUCCESS"); payment.put("pay_time","2025-08-26 19:41:14");
        payment.put("ts",System.currentTimeMillis());
        linkedList.addEvent("payment", payment);

        // ===== 处理链表并写入 DWD Kafka 表 =====
        linkedList.processEvents(tableEnv);

        // env.execute(); // 链表模拟执行，不需要真正 stream source，可注释
    }
}
