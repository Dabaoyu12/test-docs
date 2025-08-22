package com.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;

public class HBaseChainToDoris {

    private static final String DORIS_JDBC_URL = "jdbc:mysql://cdh01:9030/aaa";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASS = "";

    public static void main(String[] args) throws Exception {
        // 1️ HBase 连接
        Configuration config = HBaseConfiguration.create();
        org.apache.hadoop.hbase.client.Connection hbaseConn = ConnectionFactory.createConnection(config);

        // 2️ Doris JDBC 连接
        java.sql.Connection dorisConn = DriverManager.getConnection(DORIS_JDBC_URL, DORIS_USER, DORIS_PASS);
        dorisConn.setAutoCommit(false);

        // 3️ Doris 插入 SQL
        String insertSql = "INSERT INTO dwd_activity_sku_doris " +
                "(activity_id, sku_id, rule_id, activity_name, activity_type, condition_amount, condition_num, " +
                "benefit_amount, benefit_discount, sku_name, category1_id, category1_name, " +
                "category2_id, category2_name, category3_id, category3_name, tm_id, tm_name, start_time, end_time, create_time) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement pstmt = dorisConn.prepareStatement(insertSql);

        // 4️ 扫描 HBase 表，列簇统一用 "info"
        Map<String, Map<String, String>> activityMap = scanTable(hbaseConn, "dim_activity_info", "info");
        Map<String, List<Map<String, String>>> activityRuleMap = scanTableList(hbaseConn, "dim_activity_rule", "info", "activity_id");
        Map<String, List<Map<String, String>>> activitySkuMap = scanTableList(hbaseConn, "dim_activity_sku", "info", "activity_id");
        Map<String, Map<String, String>> skuMap = scanTable(hbaseConn, "dim_sku_info", "info");
        Map<String, Map<String, String>> spuMap = scanTable(hbaseConn, "dim_spu_info", "info");
        Map<String, Map<String, String>> category3Map = scanTable(hbaseConn, "dim_base_category3", "info");
        Map<String, Map<String, String>> category2Map = scanTable(hbaseConn, "dim_base_category2", "info");
        Map<String, Map<String, String>> category1Map = scanTable(hbaseConn, "dim_base_category1", "info");
        Map<String, Map<String, String>> trademarkMap = scanTable(hbaseConn, "dim_base_trademark", "info");

        System.out.println("HBase 表数据加载完成。");
        System.out.println("activityMap size: " + activityMap.size());
        System.out.println("activityRuleMap size: " + activityRuleMap.size());
        System.out.println("activitySkuMap size: " + activitySkuMap.size());
        System.out.println("skuMap size: " + skuMap.size());

        int batchSize = 0;
        final int BATCH_LIMIT = 100;
        int totalWritten = 0;

        // 5遍历 activitySku 表做多对多 join
        for (Map.Entry<String, List<Map<String, String>>> entry : activitySkuMap.entrySet()) {
            String activityId = entry.getKey();
            List<Map<String, String>> skuRows = entry.getValue();
            Map<String, String> activity = activityMap.get(activityId);

            if (activity == null) {
                System.out.println("Missing activity: " + activityId);
                continue;
            }

            List<Map<String, String>> rules = activityRuleMap.get(activityId);
            if (rules == null || rules.isEmpty()) {
                rules = Collections.singletonList(new HashMap<>()); // 空 rule 也可插入
            }

            for (Map<String, String> skuRow : skuRows) {
                String skuId = skuRow.get("sku_id");
                if (skuId == null) continue;
                Map<String, String> sku = skuMap.get(skuId);
                if (sku == null) {
                    System.out.println("Missing sku: " + skuId);
                    continue;
                }

                Map<String, String> spu = spuMap.get(sku.get("spu_id"));
                Map<String, String> category3 = category3Map.get(sku.get("category3_id"));
                Map<String, String> category2 = category2Map.get(category3 != null ? category3.get("category2_id") : null);
                Map<String, String> category1 = category1Map.get(category2 != null ? category2.get("category1_id") : null);
                Map<String, String> tm = trademarkMap.get(sku.get("tm_id"));

                for (Map<String, String> rule : rules) {
                    pstmt.setString(1, activity.get("id"));
                    pstmt.setString(2, sku.get("id"));
                    pstmt.setString(3, rule != null ? rule.get("id") : null);
                    pstmt.setString(4, activity.get("activity_name"));
                    pstmt.setString(5, activity.get("activity_type"));
                    pstmt.setString(6, rule != null ? rule.get("condition_amount") : null);
                    pstmt.setString(7, rule != null ? rule.get("condition_num") : null);
                    pstmt.setString(8, rule != null ? rule.get("benefit_amount") : null);
                    pstmt.setString(9, rule != null ? rule.get("benefit_discount") : null);
                    pstmt.setString(10, sku.get("sku_name"));
                    pstmt.setString(11, category1 != null ? category1.get("id") : null);
                    pstmt.setString(12, category1 != null ? category1.get("name") : null);
                    pstmt.setString(13, category2 != null ? category2.get("id") : null);
                    pstmt.setString(14, category2 != null ? category2.get("name") : null);
                    pstmt.setString(15, category3 != null ? category3.get("id") : null);
                    pstmt.setString(16, category3 != null ? category3.get("name") : null);
                    pstmt.setString(17, tm != null ? tm.get("id") : null);
                    pstmt.setString(18, tm != null ? tm.get("tm_name") : null);
                    pstmt.setString(19, activity.get("start_time"));
                    pstmt.setString(20, activity.get("end_time"));
                    pstmt.setString(21, activity.get("create_time"));

                    pstmt.addBatch();
                    batchSize++;
                    System.out.println("Adding batch: activity=" + activityId + ", sku=" + skuId + ", rule=" + (rule != null ? rule.get("id") : "null"));

                    if (batchSize >= BATCH_LIMIT) {
                        int[] counts = pstmt.executeBatch();
                        dorisConn.commit();
                        totalWritten += sum(counts);
                        pstmt.clearBatch();
                        batchSize = 0;
                    }
                }
            }
        }

        if (batchSize > 0) {
            int[] counts = pstmt.executeBatch();
            dorisConn.commit();
            totalWritten += sum(counts);
        }

        System.out.println("数据链表处理完成并写入 Doris，总写入条数: " + totalWritten);

        pstmt.close();
        dorisConn.close();
        hbaseConn.close();
    }

    // 扫描 HBase 单条 Map
    private static Map<String, Map<String, String>> scanTable(
            org.apache.hadoop.hbase.client.Connection hbaseConn, String tableNameStr, String cf) throws Exception {
        TableName tableName = TableName.valueOf(tableNameStr);
        Table table = hbaseConn.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Map<String, Map<String, String>> map = new HashMap<>();
        for (Result r : scanner) {
            String rowKey = Bytes.toString(r.getRow());
            Map<String, String> rowMap = new HashMap<>();
            if (r.getFamilyMap(Bytes.toBytes(cf)) != null) {
                for (byte[] qualifier : r.getFamilyMap(Bytes.toBytes(cf)).keySet()) {
                    rowMap.put(Bytes.toString(qualifier), Bytes.toString(r.getValue(Bytes.toBytes(cf), qualifier)));
                }
            }
            if (!rowMap.isEmpty()) map.put(rowKey, rowMap);
        }
        scanner.close();
        table.close();
        return map;
    }

    // 扫描 HBase 并按 groupKey 分组为 List
    private static Map<String, List<Map<String, String>>> scanTableList(
            org.apache.hadoop.hbase.client.Connection hbaseConn, String tableNameStr, String cf, String groupKey) throws Exception {
        TableName tableName = TableName.valueOf(tableNameStr);
        Table table = hbaseConn.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Map<String, List<Map<String, String>>> map = new HashMap<>();
        for (Result r : scanner) {
            Map<String, String> rowMap = new HashMap<>();
            if (r.getFamilyMap(Bytes.toBytes(cf)) != null) {
                for (byte[] qualifier : r.getFamilyMap(Bytes.toBytes(cf)).keySet()) {
                    rowMap.put(Bytes.toString(qualifier), Bytes.toString(r.getValue(Bytes.toBytes(cf), qualifier)));
                }
            }
            if (!rowMap.isEmpty() && rowMap.get(groupKey) != null) {
                String key = rowMap.get(groupKey);
                map.computeIfAbsent(key, k -> new ArrayList<>()).add(rowMap);
            }
        }
        scanner.close();
        table.close();
        return map;
    }

    private static int sum(int[] counts) {
        int s = 0;
        for (int c : counts) s += c;
        return s;
    }
}
