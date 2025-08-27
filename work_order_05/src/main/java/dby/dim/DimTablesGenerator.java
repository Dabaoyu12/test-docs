package dby.dim;

import java.sql.*;


public class DimTablesGenerator {

    public static void main(String[] args) throws Exception {
        // 加载 Hive JDBC Driver
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        // Hive 配置
        String hiveUrl = "jdbc:hive2://cdh01:10000/work_order_05";
        String hiveUser = "root";
        String hivePass = "123456";

        // 连接 Hive
        Connection hiveConn = DriverManager.getConnection(hiveUrl, hiveUser, hivePass);
        Statement hiveStmt = hiveConn.createStatement();

        // 1. 创建维度表
        String createDimUser = "CREATE TABLE IF NOT EXISTS dim_user (" +
                "user_id STRING, register_time TIMESTAMP, gender STRING, age INT, region STRING, device_type STRING" +
                ") STORED AS PARQUET";
        hiveStmt.execute(createDimUser);

        String createDimShop = "CREATE TABLE IF NOT EXISTS dim_shop (" +
                "shop_id STRING, shop_name STRING, seller_id STRING, category_id STRING, create_time TIMESTAMP" +
                ") STORED AS PARQUET";
        hiveStmt.execute(createDimShop);

        String createDimItem = "CREATE TABLE IF NOT EXISTS dim_item (" +
                "item_id STRING, sku_id STRING, shop_id STRING, category_id STRING, item_name STRING, price DECIMAL(10,2)" +
                ") STORED AS PARQUET";
        hiveStmt.execute(createDimItem);

        String createDimSku = "CREATE TABLE IF NOT EXISTS dim_sku (" +
                "sku_id STRING, item_id STRING, shop_id STRING, price DECIMAL(10,2)" +
                ") STORED AS PARQUET";
        hiveStmt.execute(createDimSku);

        // 2. 从 ODS 表读取数据并填充维度表
        hiveStmt.execute("INSERT INTO TABLE dim_user " +
                "SELECT user_id, register_time, gender, age, region, device_type FROM ods_user");

        hiveStmt.execute("INSERT INTO TABLE dim_shop " +
                "SELECT shop_id, shop_name, seller_id, category_id, create_time FROM ods_shop");

        hiveStmt.execute("INSERT INTO TABLE dim_item " +
                "SELECT item_id, sku_id, shop_id, category_id, item_name, price FROM ods_item");

        hiveStmt.execute("INSERT INTO TABLE dim_sku " +
                "SELECT sku_id, item_id, shop_id, price FROM ods_item");

        // 关闭连接
        hiveStmt.close();
        hiveConn.close();

        System.out.println("Hive 维度表创建并填充完成！");
    }
}
