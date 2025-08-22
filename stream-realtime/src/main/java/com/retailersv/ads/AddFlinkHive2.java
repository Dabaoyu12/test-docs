package com.retailersv.ads;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class AddFlinkHive2 {

    public static void main(String[] args) throws Exception {

        // 1. 初始化 Flink 流和 Table 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-ads");
        env.setParallelism(2);

        tableEnv.getConfig().getConfiguration().setString("table.exec.window-agg.buffer-size.limit", "2000000");
        tableEnv.getConfig().getConfiguration().setString("table.exec.resource.default-parallelism", "2");

        // 2. 注册 HiveCatalog
        String catalogName = "myhive";
        String defaultDatabase = "flink";
        String hiveConfDir = "E:\\IDEAAAA\\shixun1\\test-docs\\stream-realtime\\src\\main\\resources";
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog(catalogName, hiveCatalog);
        tableEnv.useCatalog(catalogName);
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + defaultDatabase);
        tableEnv.useDatabase(defaultDatabase);

        // 3. 创建 Kafka 源表（扁平化字段）
        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS dws_user_action_enriched (" +
                        " event_type STRING," +
                        " uid STRING," +
                        " ch STRING," +
                        " is_new STRING," +
                        " vc STRING," +
                        " ar STRING," +
                        " page_id STRING," +
                        " last_page_id STRING," +
                        " display_item STRING," +
                        " ts BIGINT," +
                        " proctime AS PROCTIME()" +
                        ") WITH (" +
                        " 'connector' = 'kafka'," +
                        " 'topic' = 'dws_user_action_enriched'," +
                        " 'properties.bootstrap.servers' = 'cdh01:9092'," +
                        " 'properties.group.id' = 'ads_user_behavior_window'," +
                        " 'scan.startup.mode' = 'earliest-offset'," +
                        " 'format' = 'json'," +
                        " 'json.ignore-parse-errors' = 'true'" +
                        ")"
        );

        // 4. PV/UV 10s 窗口
        Table pvUvTable = tableEnv.sqlQuery(
                "SELECT " +
                        " DATE_FORMAT(window_start,'yyyy-MM-dd HH:mm:ss') AS stt," +
                        " DATE_FORMAT(window_end,'yyyy-MM-dd HH:mm:ss') AS edt," +
                        " vc, ch, ar, is_new," +
                        " COUNT(*) AS pv_ct," +
                        " COUNT(DISTINCT uid) AS uv_ct," +
                        " DATE_FORMAT(window_start,'yyyy-MM-dd') AS dt " +
                        "FROM TABLE(TUMBLE(TABLE dws_user_action_enriched, DESCRIPTOR(proctime), INTERVAL '10' SECOND)) " +
                        "GROUP BY window_start, window_end, vc, ch, ar, is_new"
        );
        tableEnv.createTemporaryView("pv_uv_tmp", pvUvTable);

        // 5. 会话指标 10s 窗口
        Table sessionTable = tableEnv.sqlQuery(
                "SELECT " +
                        " DATE_FORMAT(window_start,'yyyy-MM-dd HH:mm:ss') AS stt," +
                        " DATE_FORMAT(window_end,'yyyy-MM-dd HH:mm:ss') AS edt," +
                        " vc, ch, ar, is_new," +
                        " COUNT(*) AS sv_ct," +
                        " SUM(CASE WHEN last_page_id IS NULL THEN 1 ELSE 0 END) AS uj_ct," +
                        " DATE_FORMAT(window_start,'yyyy-MM-dd') AS dt " +
                        "FROM TABLE(TUMBLE(TABLE dws_user_action_enriched, DESCRIPTOR(proctime), INTERVAL '10' SECOND)) " +
                        "WHERE event_type = 'start' " +
                        "GROUP BY window_start, window_end, vc, ch, ar, is_new"
        );
        tableEnv.createTemporaryView("session_tmp", sessionTable);

        // 6. 曝光/点击 10s 窗口
        Table displayClickTable = tableEnv.sqlQuery(
                "SELECT " +
                        " DATE_FORMAT(window_start,'yyyy-MM-dd HH:mm:ss') AS stt," +
                        " DATE_FORMAT(window_end,'yyyy-MM-dd HH:mm:ss') AS edt," +
                        " display_item AS sku_id," +
                        " page_id," +
                        " ch," +
                        " COUNT(CASE WHEN event_type='display' THEN 1 END) AS disp_ct," +
                        " COUNT(DISTINCT CASE WHEN event_type='display' THEN display_item END) AS disp_sku_num," +
                        " DATE_FORMAT(window_start,'yyyy-MM-dd') AS dt " +
                        "FROM TABLE(TUMBLE(TABLE dws_user_action_enriched, DESCRIPTOR(proctime), INTERVAL '10' SECOND)) " +
                        "WHERE event_type IN ('display','click') AND display_item IS NOT NULL " +
                        "GROUP BY window_start, window_end, display_item, page_id, ch"
        );
        tableEnv.createTemporaryView("disp_click_tmp", displayClickTable);

        // 7. Hive 分区表 commit policy 配置
        String hiveSink = "'connector'='hive'," +
                "'format'='parquet'," +
                "'sink.partition-commit.policy.kind'='metastore,success-file'," +
                "'sink.partition-commit.trigger'='process-time'," +
                "'sink.partition-commit.delay'='0s'";

        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS ads_user_pv_uv_window (" +
                        " stt STRING," +
                        " edt STRING," +
                        " vc STRING," +
                        " ch STRING," +
                        " ar STRING," +
                        " is_new STRING," +
                        " pv_ct BIGINT," +
                        " uv_ct BIGINT" +
                        ") PARTITIONED BY (dt) WITH (" + hiveSink + ")"
        );

        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS ads_user_session_window (" +
                        " stt STRING," +
                        " edt STRING," +
                        " vc STRING," +
                        " ch STRING," +
                        " ar STRING," +
                        " is_new STRING," +
                        " sv_ct BIGINT," +
                        " uj_ct BIGINT" +
                        ") PARTITIONED BY (dt) WITH (" + hiveSink + ")"
        );

        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS ads_user_display_click_window (" +
                        " stt STRING," +
                        " edt STRING," +
                        " sku_id STRING," +
                        " page_id STRING," +
                        " ch STRING," +
                        " disp_ct BIGINT," +
                        " disp_sku_num BIGINT" +
                        ") PARTITIONED BY (dt) WITH (" + hiveSink + ")"
        );

        // 8. 写入 Hive
        tableEnv.executeSql("INSERT INTO ads_user_pv_uv_window SELECT stt, edt, vc, ch, ar, is_new, pv_ct, uv_ct, dt FROM pv_uv_tmp");
        tableEnv.executeSql("INSERT INTO ads_user_session_window SELECT stt, edt, vc, ch, ar, is_new, sv_ct, uj_ct, dt FROM session_tmp");
        tableEnv.executeSql("INSERT INTO ads_user_display_click_window SELECT stt, edt, sku_id, page_id, ch, disp_ct, disp_sku_num, dt FROM disp_click_tmp");

        // 9. 启动作业
        env.execute("AdsUserBehaviorWindowHiveJob");
    }
}
