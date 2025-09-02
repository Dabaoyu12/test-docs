package dby.dws;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class DwsAdminJob {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. MySQL CDC Source
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("cdh01")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("work_order_05")
                .tableList(
                        "work_order_05.ods_order",
                        "work_order_05.ods_order_item",
                        "work_order_05.ods_payment",
                        "work_order_05.ods_refund",
                        "work_order_05.ods_user",
                        "work_order_05.ods_item",
                        "work_order_05.ods_shop"
                )
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> sourceStream = env.fromSource(
                mysqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL Source"
        );

        // 3. 业务逻辑示例：加购件数 Top50
        SingleOutputStreamOperator<Tuple3<String, String, Long>> cartCnt = sourceStream
                .map(o -> {
                    String shopId = "shop1";
                    String itemId = "item1";
                    Long quantity = 1L;
                    return new Tuple3<>(shopId, itemId, quantity);
                })
                .returns(new TypeHint<Tuple3<String, String, Long>>() {})
                .keyBy(t -> t.f0 + "_" + t.f1)
                .sum(2);

        SingleOutputStreamOperator<String> cartTop50 = cartCnt
                .map(t -> toJsonMetric("cart_top50", t.f0, t.f1, t.f2));

        // 4. 支付金额 Top50
        SingleOutputStreamOperator<Tuple3<String, String, Long>> payAmount = sourceStream
                .map(o -> {
                    String shopId = "shop1";
                    String itemId = "item1";
                    Long pay = 100L;
                    Long refund = 0L;
                    return new Tuple3<>(shopId, itemId, pay - refund);
                })
                .returns(new TypeHint<Tuple3<String, String, Long>>() {})
                .keyBy(t -> t.f0 + "_" + t.f1)
                .sum(2);

        SingleOutputStreamOperator<String> payTop50 = payAmount
                .map(t -> toJsonMetric("pay_top50", t.f0, t.f1, t.f2));

        // 5. 输出到控制台
        cartTop50.print("CartTop50");
        payTop50.print("PayTop50");

        env.execute("DwsAdminJob");
    }

    private static String toJsonMetric(String metric, String shopId, String itemId, Long value) {
        return String.format("{\"metric\":\"%s\",\"shopId\":\"%s\",\"itemId\":\"%s\",\"value\":%d}",
                metric, shopId, itemId, value);
    }
}
