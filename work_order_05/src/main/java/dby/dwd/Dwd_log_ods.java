package dby.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Dwd_log_ods {
    public static void main(String[] args) throws Exception {
        // 初始化 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 公共 Kafka 配置
        String kafkaServers = "cdh01:9092";

        // ===== 1. 定义 ODS 源表 (Kafka) =====
        String[] odsTopics = {
                "ods_start_log_05",
                "ods_page_log_05",
                "ods_display_log_05",
                "ods_action_log_05",
                "ods_error_log_05"
        };

        for (String topic : odsTopics) {
            String createSource = String.format(
                    "CREATE TABLE %s (" +
                            " shop_id STRING, " +
                            " page_id STRING, " +
                            " event_id STRING, " +
                            " event_type STRING, " +
                            " quantity INT, " +
                            " device_id STRING, " +
                            " user_id STRING, " +
                            " item_id STRING, " +
                            " session_id STRING, " +
                            " event_time STRING, " +  // 源是字符串
                            " props MAP<STRING, STRING>, " +
                            // 转换成 TIMESTAMP_LTZ
                            " ts AS CAST(event_time AS TIMESTAMP_LTZ(3)), " +
                            " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND " +
                            ") WITH (" +
                            " 'connector' = 'kafka'," +
                            " 'topic' = '" + topic + "'," +
                            " 'properties.bootstrap.servers' = '" + kafkaServers + "'," +
                            " 'properties.group.id' = 'log_ods_dwd_group'," +
                            " 'format' = 'json'," +
                            " 'scan.startup.mode' = 'earliest-offset'" +
                            ")",
                    topic
            );
            tableEnv.executeSql(createSource);
        }

        // ===== 2. 定义 DWD Sink 表 =====
        String[] dwdTopics = {
                "dwd_start_log_05",
                "dwd_page_log_05",
                "dwd_display_log_05",
                "dwd_action_log_05",
                "dwd_error_log_05"
        };

        for (String topic : dwdTopics) {
            String createSink = String.format(
                    "CREATE TABLE %s (" +
                            " shop_id STRING, " +
                            " page_id STRING, " +
                            " event_id STRING, " +
                            " event_type STRING, " +
                            " quantity INT, " +
                            " device_id STRING, " +
                            " user_id STRING, " +
                            " item_id STRING, " +
                            " session_id STRING, " +
                            " event_time STRING, " +
                            " props MAP<STRING, STRING>, " +
                            " ts TIMESTAMP_LTZ(3) " +
                            ") WITH (" +
                            " 'connector' = 'kafka'," +
                            " 'topic' = '" + topic + "'," +
                            " 'properties.bootstrap.servers' = '" + kafkaServers + "'," +
                            " 'format' = 'json'" +
                            ")",
                    topic
            );
            tableEnv.executeSql(createSink);
        }

        // ===== 3. 清洗逻辑 & 插入 DWD =====
        for (int i = 0; i < odsTopics.length; i++) {
            String ods = odsTopics[i];
            String dwd = dwdTopics[i];
            String insertSql = String.format(
                    "INSERT INTO %s " +
                            "SELECT shop_id, page_id, event_id, event_type, quantity, " +
                            "       device_id, user_id, item_id, session_id, event_time, props, ts " +
                            "FROM %s " +
                            "WHERE event_id IS NOT NULL " +
                            "  AND event_time IS NOT NULL " +
                            "  AND user_id IS NOT NULL " +
                            "  AND device_id IS NOT NULL",
                    dwd, ods
            );
            tableEnv.executeSql(insertSql);
        }

        System.out.println("ODS -> DWD Log Cleansing Job started...");
    }
}
