package dby.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class ReadWorkOrder05 {
    public static void main(String[] args) throws Exception {
        // 1. 初始化 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Kafka 配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cdh01:9092");
        properties.setProperty("group.id", "read_work_order_05_group");

        // 3. 定义 Kafka 消费者 (直接读字符串)
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "work_order_05",
                new SimpleStringSchema(),
                properties
        );
        consumer.setStartFromEarliest();  // 从最早开始消费，便于测试

        // 4. 添加数据源
        DataStream<String> stream = env.addSource(consumer);


        stream
                .map(value -> "源数据: " + value)
                .print();

        // 6. 启动作业
        env.execute("Read Raw work_order_05 Kafka Data");
    }
}
