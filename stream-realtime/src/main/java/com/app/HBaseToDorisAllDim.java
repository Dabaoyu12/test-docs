package com.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.LinkedHashMap;
import java.util.Map;

public class HBaseToDorisAllDim {

    private static final String HBASE_ZK = "cdh01:2181,cdh02:2181,cdh03:2181";
    private static final String DORIS_URL = "jdbc:mysql://cdh01:9030/aaa";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASS = "";

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", HBASE_ZK);

        try (org.apache.hadoop.hbase.client.Connection hConn = ConnectionFactory.createConnection(conf);
             Connection dConn = DriverManager.getConnection(DORIS_URL, DORIS_USER, DORIS_PASS)) {

            dConn.setAutoCommit(false);

            // === 同步所有维度表 ===
            syncTable(hConn, dConn, "dim_activity_info", new String[]{
                    "id","activity_name","activity_type","activity_desc","start_time","end_time","create_time"
            });

            syncTable(hConn, dConn, "dim_activity_rule", new String[]{
                    "id","activity_id","activity_type","condition_amount","condition_num","benefit_amount",
                    "benefit_discount","benefit_level"
            });

            syncTable(hConn, dConn, "dim_activity_sku", new String[]{
                    "id","activity_id","sku_id","create_time"
            });

            syncTable(hConn, dConn, "dim_base_category1", new String[]{
                    "id","name"
            });

            syncTable(hConn, dConn, "dim_base_category2", new String[]{
                    "id","name","category1_id"
            });

            syncTable(hConn, dConn, "dim_base_category3", new String[]{
                    "id","name","category2_id"
            });

            syncTable(hConn, dConn, "dim_base_dic", new String[]{
                    "dic_code","dic_name"
            });

            syncTable(hConn, dConn, "dim_base_province", new String[]{
                    "id","name","region_id","area_code","iso_code","iso_3166_2"
            });

            syncTable(hConn, dConn, "dim_base_region", new String[]{
                    "id","region_name"
            });

            syncTable(hConn, dConn, "dim_base_trademark", new String[]{
                    "id","tm_name"
            });

            syncTable(hConn, dConn, "dim_coupon_info", new String[]{
                    "id","coupon_name","coupon_type","condition_amount","condition_num","activity_id",
                    "benefit_amount","benefit_discount","create_time","range_type","limit_num",
                    "taken_count","start_time","end_time","operate_time","expire_time","range_desc"
            });

            syncTable(hConn, dConn, "dim_coupon_range", new String[]{
                    "id","coupon_id","range_type","range_id"
            });

            syncTable(hConn, dConn, "dim_financial_sku_cost", new String[]{
                    "id","sku_id","sku_name","busi_date","is_lastest","sku_cost","create_time"
            });

            syncTable(hConn, dConn, "dim_sku_info", new String[]{
                    "id","spu_id","price","sku_name","sku_desc","weight","tm_id","category3_id",
                    "sku_default_img","is_sale","create_time"
            });

            syncTable(hConn, dConn, "dim_spu_info", new String[]{
                    "id","spu_name","description","category3_id","tm_id"
            });

            syncTable(hConn, dConn, "dim_user_info", new String[]{
                    "id","login_name","name","user_level","birthday","gender","create_time","operate_time"
            });

            System.out.println("所有维度表同步完成！");

        }
    }

    /**
     * 通用 HBase → Doris 同步方法
     */
    private static void syncTable(org.apache.hadoop.hbase.client.Connection hConn,
                                  Connection dConn,
                                  String tableName,
                                  String[] columns) throws Exception {
        Table hTable = hConn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);

        // 组装 SQL
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        for (int i = 0; i < columns.length; i++) {
            sql.append(columns[i]);
            if (i < columns.length - 1) sql.append(",");
        }
        sql.append(") VALUES (");
        for (int i = 0; i < columns.length; i++) {
            sql.append("?");
            if (i < columns.length - 1) sql.append(",");
        }
        sql.append(")");

        PreparedStatement pstmt = dConn.prepareStatement(sql.toString());

        int batchSize = 0, total = 0;
        for (Result r : scanner) {
            Map<String,String> row = new LinkedHashMap<>();
            for (String col : columns) {
                String val = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes(col)));
                row.put(col, val);
            }

            if (row.get(columns[0]) == null) continue; // 跳过主键为空

            for (int i = 0; i < columns.length; i++) {
                pstmt.setString(i + 1, row.get(columns[i]));
            }
            pstmt.addBatch();
            batchSize++;

            if (batchSize >= 500) {
                pstmt.executeBatch();
                pstmt.clearBatch();
                dConn.commit();
                total += batchSize;
                batchSize = 0;
            }
        }
        if (batchSize > 0) {
            pstmt.executeBatch();
            dConn.commit();
            total += batchSize;
        }

        pstmt.close();
        hTable.close();
        scanner.close();

        System.out.println("表 " + tableName + " 同步完成，共写入 " + total + " 条。");
    }
}
