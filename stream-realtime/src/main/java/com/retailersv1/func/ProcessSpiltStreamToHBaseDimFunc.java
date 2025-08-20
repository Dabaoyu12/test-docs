package com.retailersv1.func;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

/**
 * 将一条埋点 JSON 拆分为四类日志（start/page/display/action），
 * 统一输出到下游（用于写入 Kafka 的 dwd_user_behavior_log），
 * 并按配置写入 HBase（sink_table、sink_cf 可从广播配置获取）。
 *
 * 广播配置（举例）：
 * {
 *   "source_table": "dwd_user_behavior_log",
 *   "sink_table": "dwd_user_behavior_log_hbase",
 *   "sink_cf": "info"
 * }
 */
public class ProcessSpiltStreamToHBaseDimFunc extends BroadcastProcessFunction<JSONObject, JSONObject, JSONObject> {

    private final MapStateDescriptor<String, JSONObject> mapStateDesc;

    // HBase 连接
    private transient Connection connection;

    public ProcessSpiltStreamToHBaseDimFunc(MapStateDescriptor<String, JSONObject> mapStateDesc) {
        this.mapStateDesc = mapStateDesc;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        // 正确配置：quorum 写主机名列表，端口单独配
        conf.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03"); // TODO 按你的实际环境修改
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(conf);
    }

    @Override
    public void processElement(JSONObject value,
                               ReadOnlyContext ctx,
                               Collector<JSONObject> out) throws Exception {

        if (value == null || value.isEmpty()) {
            return;
        }

        // ===== 1) 解析表名与数据主体（兼容 CDC / 直传两种格式） =====
        JSONObject data = value.getJSONObject("after");
        String table = value.getString("table");
        if (data == null || table == null) {
            // 非 CDC，直接用原始 JSON；统一认为来源表就是 dwd_user_behavior_log
            data = value;
            table = "dwd_user_behavior_log";
        }

        // ===== 2) 读取广播配置（sink_table、列族等） =====
        ReadOnlyBroadcastState<String, JSONObject> configState = ctx.getBroadcastState(mapStateDesc);
        JSONObject config = configState.get(table);
        if (config == null) {
            // 如果没配置，走默认值
            config = new JSONObject();
            config.put("source_table", table);
            config.put("sink_table", "dwd_user_behavior_log_hbase"); // 默认 HBase 表名，可按需改
            config.put("sink_cf", "info");                            // 默认列族
        }
        final String sinkTable = config.getString("sink_table");
        final String sinkCF = config.getString("sink_cf") == null ? "info" : config.getString("sink_cf");

        // ===== 3) 拆分 4 类日志 =====
        JSONObject common = data.getJSONObject("common"); // 公共字段
        JSONObject start  = data.getJSONObject("start");
        JSONObject page   = data.getJSONObject("page");
        JSONArray displays = data.getJSONArray("displays");
        JSONArray actions  = data.getJSONArray("actions");
        Long rootTs = data.getLong("ts");

        // ---- 3.1 启动日志 start ----
        if (start != null) {
            JSONObject startLog = new JSONObject(true);
            putAllSafe(startLog, common);
            putAllSafe(startLog, start);
            // 统一标记
            startLog.put("log_type", "start");
            // ts：优先各自 ts（若有），否则用根 ts
            startLog.put("ts", start.getLongValue("ts") > 0 ? start.getLongValue("ts") : rootTs);
            // 用于下游区分 topic 的字段（可选）
            startLog.put("sink_table", sinkTable);
            startLog.put("sink_cf", sinkCF);

            out.collect(startLog);
            writeOneToHBase(sinkTable, sinkCF, startLog, "start");
        }

        // ---- 3.2 页面日志 page ----
        if (page != null) {
            JSONObject pageLog = new JSONObject(true);
            putAllSafe(pageLog, common);
            putAllSafe(pageLog, page);
            pageLog.put("log_type", "page");
            pageLog.put("ts", page.getLongValue("ts") > 0 ? page.getLongValue("ts") : rootTs);
            pageLog.put("sink_table", sinkTable);
            pageLog.put("sink_cf", sinkCF);

            out.collect(pageLog);
            writeOneToHBase(sinkTable, sinkCF, pageLog, "page");
        }

        // ---- 3.3 曝光日志 displays[] ----
        if (displays != null && !displays.isEmpty()) {
            for (int i = 0; i < displays.size(); i++) {
                JSONObject disp = displays.getJSONObject(i);
                JSONObject displayLog = new JSONObject(true);
                putAllSafe(displayLog, common);
                // 曝光常需要 page_id 语境
                if (page != null && page.containsKey("page_id")) {
                    displayLog.put("page_id", page.getString("page_id"));
                }
                putAllSafe(displayLog, disp);
                displayLog.put("log_type", "display");
                displayLog.put("ts", disp.getLongValue("ts") > 0 ? disp.getLongValue("ts") : rootTs);
                displayLog.put("sink_table", sinkTable);
                displayLog.put("sink_cf", sinkCF);

                out.collect(displayLog);
                writeOneToHBase(sinkTable, sinkCF, displayLog, "display");
            }
        }

        // ---- 3.4 动作日志 actions[]（关键：新增动作输出） ----
        if (actions != null && !actions.isEmpty()) {
            for (int i = 0; i < actions.size(); i++) {
                JSONObject act = actions.getJSONObject(i);
                JSONObject actionLog = new JSONObject(true);
                putAllSafe(actionLog, common);
                // 动作也常带 page_id
                if (page != null && page.containsKey("page_id")) {
                    actionLog.put("page_id", page.getString("page_id"));
                }
                putAllSafe(actionLog, act);
                actionLog.put("log_type", "action");
                actionLog.put("ts", act.getLongValue("ts") > 0 ? act.getLongValue("ts") : rootTs);
                actionLog.put("sink_table", sinkTable);
                actionLog.put("sink_cf", sinkCF);

                out.collect(actionLog);
                writeOneToHBase(sinkTable, sinkCF, actionLog, "action");
            }
        }
    }

    @Override
    public void processBroadcastElement(JSONObject value,
                                        Context ctx,
                                        Collector<JSONObject> out) throws Exception {
        if (value == null || value.isEmpty()) return;
        String sourceTable = value.getString("source_table");
        if (sourceTable == null) return;
        // 支持配置 sink_table 与 sink_cf
        ctx.getBroadcastState(mapStateDesc).put(sourceTable, value);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    /* ====================== 工具方法 ====================== */

    /** 安全地 putAll：null 忽略 */
    private static void putAllSafe(JSONObject target, JSONObject src) {
        if (target == null || src == null || src.isEmpty()) return;
        Set<String> keys = src.keySet();
        for (String k : keys) {
            target.put(k, src.get(k));
        }
    }

    /**
     * 将一条日志写入 HBase
     * rowkey 设计：{sidOrMid}_{logType}_{ts}_{8位uuid}
     */
    private void writeOneToHBase(String sinkTable, String cf, JSONObject record, String logType) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(sinkTable));

            String sid = record.getString("sid");
            String mid = record.getString("mid");
            long ts = record.getLongValue("ts");
            String uniq = UUID.randomUUID().toString().substring(0, 8);
            String prefix = (sid != null && !sid.isEmpty()) ? sid : (mid != null ? mid : "na");
            String rowKey = prefix + "_" + logType + "_" + ts + "_" + uniq;

            Put put = new Put(Bytes.toBytes(rowKey));

            // 遍历写入所有字段（排除 sink_table/sink_cf 避免脏字段）
            Iterator<String> it = record.keySet().iterator();
            while (it.hasNext()) {
                String key = it.next();
                if ("sink_table".equals(key) || "sink_cf".equals(key)) continue;
                Object v = record.get(key);
                if (v != null) {
                    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(key), Bytes.toBytes(String.valueOf(v)));
                }
            }
            table.put(put);
        } catch (Exception e) {
            System.err.println("HBase put error, table=" + sinkTable + ", msg=" + e.getMessage());
        } finally {
            if (table != null) {
                try { table.close(); } catch (Exception ignore) {}
            }
        }
    }
}
