package com.retailersv.ads;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
//import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;

public class AdsFlinkHive {
    public static void main(String[] args) throws Exception {

        // 1) 初始化 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(2);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-ads");

        // 2) 创建 Kafka 源表（模拟你的 dwd_order_detail_enriched 数据）
        tableEnv.executeSql(
                "CREATE TABLE dwd_order_detail_enriched (\n" +
                        "  sku_id STRING,\n" +
                        "  category3_id STRING,\n" +
                        "  tm_id STRING,\n" +
                        "  user_id STRING,\n" +
                        "  order_id STRING,\n" +
                        "  order_detail_id STRING,\n" +
                        "  order_amount DOUBLE,\n" +
                        "  payment_amount DOUBLE,\n" +
                        "  refund_amount DOUBLE,\n" +
                        "  event_type STRING,\n" +
                        "  proc_time AS PROCTIME()\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'dwd_order_detail_enriched',\n" +
                        "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                        "  'properties.group.id' = 'ads_trade_stats',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")"
        );

        // 3) 创建临时视图进行窗口聚合
        Table orderStatsTable = tableEnv.sqlQuery(
                "SELECT " +
                        "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt, " +
                        "  DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt, " +
                        "  sku_id, " +
                        "  COUNT(*) AS order_ct, " +
                        "  COUNT(DISTINCT user_id) AS order_user_ct, " +
                        "  SUM(CAST(1 AS BIGINT)) AS sku_num, " +
                        "  SUM(order_amount) AS order_amount " +
                        "FROM TABLE(TUMBLE(TABLE dwd_order_detail_enriched, DESCRIPTOR(proc_time), INTERVAL '10' SECOND)) " +
                        "WHERE event_type = 'order' " +
                        "GROUP BY window_start, window_end, sku_id"
        );

        Table paymentStatsTable = tableEnv.sqlQuery(
                "SELECT " +
                        "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt, " +
                        "  DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt, " +
                        "  sku_id, " +
                        "  COUNT(*) AS payment_ct, " +
                        "  COUNT(DISTINCT user_id) AS payment_user_ct, " +
                        "  ROUND(SUM(payment_amount),2) AS payment_amount " +
                        "FROM TABLE(TUMBLE(TABLE dwd_order_detail_enriched, DESCRIPTOR(proc_time), INTERVAL '10' SECOND)) " +
                        "WHERE event_type = 'payment' " +
                        "GROUP BY window_start, window_end, sku_id"
        );

        Table refundStatsTable = tableEnv.sqlQuery(
                "SELECT " +
                        "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt, " +
                        "  DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt, " +
                        "  sku_id, " +
                        "  COUNT(*) AS refund_ct, " +
                        "  COUNT(DISTINCT user_id) AS refund_user_ct, " +
                        "  SUM(refund_amount) AS refund_amount " +
                        "FROM TABLE(TUMBLE(TABLE dwd_order_detail_enriched, DESCRIPTOR(proc_time), INTERVAL '10' SECOND)) " +
                        "WHERE event_type = 'refund' " +
                        "GROUP BY window_start, window_end, sku_id"
        );

        // 4) 将中间表注册为临时视图
        tableEnv.createTemporaryView("order_stats_view", orderStatsTable);
        tableEnv.createTemporaryView("payment_stats_view", paymentStatsTable);
        tableEnv.createTemporaryView("refund_stats_view", refundStatsTable);

        // 5) 聚合 JOIN
        Table resultStatsTable = tableEnv.sqlQuery(
                "SELECT " +
                        "o.stt, o.edt, o.sku_id, o.order_ct, o.order_user_ct, o.sku_num, o.order_amount, " +
                        "p.payment_ct, p.payment_user_ct, p.payment_amount, " +
                        "r.refund_ct, r.refund_user_ct, r.refund_amount " +
                        "FROM order_stats_view o " +
                        "LEFT JOIN payment_stats_view p ON o.stt = p.stt AND o.edt = p.edt AND o.sku_id = p.sku_id " +
                        "LEFT JOIN refund_stats_view r ON o.stt = r.stt AND o.edt = r.edt AND o.sku_id = r.sku_id"
        );

        // 6) 创建 print sink
        tableEnv.executeSql(
                "CREATE TABLE enriched_trade_stats (\n" +
                        "  stt STRING, edt STRING, sku_id STRING, \n" +
                        "  order_ct BIGINT, order_user_ct BIGINT, sku_num BIGINT, order_amount DOUBLE,\n" +
                        "  payment_ct BIGINT, payment_user_ct BIGINT, payment_amount DOUBLE,\n" +
                        "  refund_ct BIGINT, refund_user_ct BIGINT, refund_amount DOUBLE\n" +
                        ") WITH (\n" +
                        "  'connector' = 'print'\n" +
                        ")"
        );

        // 7) 写入 print sink
        resultStatsTable.executeInsert("enriched_trade_stats");

        // 8) 启动作业
        resultStatsTable.executeInsert("enriched_trade_stats").await();
    }
}
