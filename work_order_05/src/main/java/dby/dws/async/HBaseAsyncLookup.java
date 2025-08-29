package dby.dws.async;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HBaseAsyncLookup extends RichAsyncFunction<JSONObject, JSONObject> {
    private final String tableName;
    private final String keyField;
    private final String family;

    private transient Connection conn;
    private transient Table table;
    private transient ExecutorService executor;

    public HBaseAsyncLookup(String tableName, String keyField, String family) {
        this.tableName = tableName;
        this.keyField = keyField;
        this.family = family;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "cdh01");
        conn = ConnectionFactory.createConnection(config);
        table = conn.getTable(TableName.valueOf(tableName));
        executor = Executors.newFixedThreadPool(10);
    }

    @Override
    public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) {
        executor.submit(() -> {
            try {
                String key = input.getString(keyField);
                if (key == null) {
                    resultFuture.complete(Collections.singleton(input));
                    return;
                }
                Get get = new Get(key.getBytes());
                Result result = table.get(get);
                if (!result.isEmpty()) {
                    result.listCells().forEach(cell -> {
                        String qualifier = new String(cell.getQualifierArray(),
                                cell.getQualifierOffset(), cell.getQualifierLength());
                        String value = new String(cell.getValueArray(),
                                cell.getValueOffset(), cell.getValueLength());
                        input.put(qualifier, value);
                    });
                }
                resultFuture.complete(Collections.singleton(input));
            } catch (Exception e) {
                e.printStackTrace();
                resultFuture.complete(Collections.singleton(input));
            }
        });
    }

    @Override
    public void close() throws Exception {
        table.close();
        conn.close();
        executor.shutdown();
    }
}
