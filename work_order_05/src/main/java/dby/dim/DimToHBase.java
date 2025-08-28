package dby.dim;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class DimToHBase {

    public static void main(String[] args) throws Exception {
        // 1. HBase 配置
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "cdh01");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        org.apache.hadoop.hbase.client.Connection hbaseConn = ConnectionFactory.createConnection(conf);
        Admin admin = hbaseConn.getAdmin();

        // 2. MySQL 配置
        String url = "jdbc:mysql://cdh01:3306/work_order_05?useSSL=false&characterEncoding=utf8";
        String user = "root";
        String pass = "123456";
        java.sql.Connection mysqlConn = DriverManager.getConnection(url, user, pass);

        // 3. 定义要创建的维度表
        String[] dimTables = {"dim_user_05", "dim_shop_05", "dim_item_05", "dim_sku_05", "dim_date_05", "dim_category_05"};

        // 4. 在 HBase 中建表（列族 cf）
        for (String table : dimTables) {
            TableName tableName = TableName.valueOf(table);
            if (!admin.tableExists(tableName)) {
                HTableDescriptor desc = new HTableDescriptor(tableName);
                desc.addFamily(new HColumnDescriptor("cf"));
                admin.createTable(desc);
                System.out.println("创建 HBase 表: " + table);
            }
        }

        Statement stmt = mysqlConn.createStatement();
        ResultSet rs;

        // ========== 同步 dim_user_05 ==========
        rs = stmt.executeQuery("SELECT user_id, register_time, gender, age, region, device_type FROM ods_user");
        Table userTable = hbaseConn.getTable(TableName.valueOf("dim_user_05"));
        while (rs.next()) {
            Put put = new Put(rs.getString("user_id").getBytes());
            put.addColumn("cf".getBytes(), "register_time".getBytes(), rs.getString("register_time").getBytes());
            put.addColumn("cf".getBytes(), "gender".getBytes(), rs.getString("gender").getBytes());
            put.addColumn("cf".getBytes(), "age".getBytes(), String.valueOf(rs.getInt("age")).getBytes());
            put.addColumn("cf".getBytes(), "region".getBytes(), rs.getString("region").getBytes());
            put.addColumn("cf".getBytes(), "device_type".getBytes(), rs.getString("device_type").getBytes());
            userTable.put(put);
        }
        userTable.close();
        rs.close();

        // ========== 同步 dim_shop_05 ==========
        rs = stmt.executeQuery("SELECT shop_id, shop_name, seller_id, category_id, create_time FROM ods_shop");
        Table shopTable = hbaseConn.getTable(TableName.valueOf("dim_shop_05"));
        while (rs.next()) {
            Put put = new Put(rs.getString("shop_id").getBytes());
            put.addColumn("cf".getBytes(), "shop_name".getBytes(), rs.getString("shop_name").getBytes());
            put.addColumn("cf".getBytes(), "seller_id".getBytes(), rs.getString("seller_id").getBytes());
            put.addColumn("cf".getBytes(), "category_id".getBytes(), rs.getString("category_id").getBytes());
            put.addColumn("cf".getBytes(), "create_time".getBytes(), rs.getString("create_time").getBytes());
            shopTable.put(put);
        }
        shopTable.close();
        rs.close();

        // ========== 同步 dim_item_05 ==========
        rs = stmt.executeQuery("SELECT item_id, sku_id, shop_id, category_id, item_name, price FROM ods_item");
        Table itemTable = hbaseConn.getTable(TableName.valueOf("dim_item_05"));
        while (rs.next()) {
            Put put = new Put(rs.getString("item_id").getBytes());
            put.addColumn("cf".getBytes(), "sku_id".getBytes(), rs.getString("sku_id").getBytes());
            put.addColumn("cf".getBytes(), "shop_id".getBytes(), rs.getString("shop_id").getBytes());
            put.addColumn("cf".getBytes(), "category_id".getBytes(), rs.getString("category_id").getBytes());
            put.addColumn("cf".getBytes(), "item_name".getBytes(), rs.getString("item_name").getBytes());
            put.addColumn("cf".getBytes(), "price".getBytes(), rs.getBigDecimal("price").toString().getBytes());
            itemTable.put(put);
        }
        itemTable.close();
        rs.close();

        // ========== 同步 dim_sku_05 ==========
        rs = stmt.executeQuery("SELECT sku_id, item_id, shop_id, unit_price FROM ods_order_item");
        Table skuTable = hbaseConn.getTable(TableName.valueOf("dim_sku_05"));
        while (rs.next()) {
            Put put = new Put(rs.getString("sku_id").getBytes());
            put.addColumn("cf".getBytes(), "item_id".getBytes(), rs.getString("item_id").getBytes());
            put.addColumn("cf".getBytes(), "shop_id".getBytes(), rs.getString("shop_id").getBytes());
            put.addColumn("cf".getBytes(), "price".getBytes(), rs.getBigDecimal("unit_price").toString().getBytes());
            skuTable.put(put);
        }
        skuTable.close();
        rs.close();

        // ========== 生成 dim_date_05 ==========
        Table dateTable = hbaseConn.getTable(TableName.valueOf("dim_date_05"));
        for (int year = 2020; year <= 2025; year++) {
            for (int month = 1; month <= 12; month++) {
                for (int day = 1; day <= 28; day++) { // 简化
                    String dateKey = String.format("%04d%02d%02d", year, month, day);
                    Put put = new Put(dateKey.getBytes());
                    put.addColumn("cf".getBytes(), "year".getBytes(), String.valueOf(year).getBytes());
                    put.addColumn("cf".getBytes(), "month".getBytes(), String.valueOf(month).getBytes());
                    put.addColumn("cf".getBytes(), "day".getBytes(), String.valueOf(day).getBytes());
                    dateTable.put(put);
                }
            }
        }
        dateTable.close();

        // ========== 同步 dim_category_05 ==========
        rs = stmt.executeQuery("SELECT DISTINCT category_id FROM ods_item");
        Table catTable = hbaseConn.getTable(TableName.valueOf("dim_category_05"));
        while (rs.next()) {
            String catId = rs.getString("category_id");
            if (catId != null) {
                Put put = new Put(catId.getBytes());
                put.addColumn("cf".getBytes(), "category_name".getBytes(), ("类目_" + catId).getBytes());
                catTable.put(put);
            }
        }
        catTable.close();
        rs.close();

        // 关闭连接
        stmt.close();
        mysqlConn.close();
        hbaseConn.close();

        System.out.println("DIM 层维度表同步到 HBase 完成！");
    }
}
