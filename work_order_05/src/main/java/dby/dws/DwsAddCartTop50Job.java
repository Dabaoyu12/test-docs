package dby.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.Properties;
import java.util.stream.Collectors;

public class DwsAddCartTop50Job {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Kafka 配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "cdh01:9092");
        props.setProperty("group.id", "dws_add_cart_top50_debug");

        // 读取 dwd_cart_05
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "dwd_cart_05",
                new SimpleStringSchema(),
                props
        );
        consumer.setStartFromEarliest();

        DataStream<String> source = env.addSource(consumer);

        DataStream<JSONObject> jsonStream = source.map(JSON::parseObject);

        // 提取 item_id 和 shop_id
        DataStream<Tuple2<String, String>> itemStream = jsonStream
                .map(x -> new Tuple2<>(x.getString("item_id"), x.getString("shop_id")))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, String>>(){}));

        // 统计 item_id Top50
        itemStream
                .timeWindowAll(Time.seconds(10))
                .process(new TopNProcessAllWindowFunction<>(50, "item_id"))
                .returns(TypeInformation.of(String.class))
                .print();

        // 统计 shop_id Top50
        itemStream
                .map(t -> t.f1)
                .returns(String.class)
                .timeWindowAll(Time.seconds(10))
                .process(new TopNProcessAllWindowFunction<>(50, "shop_id"))
                .returns(TypeInformation.of(String.class))
                .print();

        env.execute("DWS Add Cart Top50 Debug Job");
    }

    // TopN 处理函数，支持泛型输入
    public static class TopNProcessAllWindowFunction<IN> extends ProcessAllWindowFunction<IN, String, TimeWindow> {
        private final int topSize;
        private final String keyName;

        public TopNProcessAllWindowFunction(int topSize, String keyName) {
            this.topSize = topSize;
            this.keyName = keyName;
        }

        @Override
        public void process(Context context, Iterable<IN> elements, Collector<String> out) {
            Map<String, Long> countMap = new HashMap<>();
            for (IN t : elements) {
                String key;
                if (t instanceof Tuple2) {
                    key = ((Tuple2<String, String>) t).f0;
                } else if (t instanceof String) {
                    key = (String) t;
                } else {
                    continue;
                }
                countMap.put(key, countMap.getOrDefault(key, 0L) + 1);
            }

            List<Map.Entry<String, Long>> topList = countMap.entrySet().stream()
                    .sorted((e1, e2) -> Long.compare(e2.getValue(), e1.getValue()))
                    .limit(topSize)
                    .collect(Collectors.toList());

            JSONObject result = new JSONObject();
            result.put("key", keyName);
            result.put("top", topList);
            result.put("window_end", context.window().getEnd());

            out.collect(result.toJSONString());
        }
    }

    // 简单 Tuple2
    public static class Tuple2<F0, F1> {
        public F0 f0;
        public F1 f1;
        public Tuple2(F0 f0, F1 f1) {
            this.f0 = f0;
            this.f1 = f1;
        }
    }
}
