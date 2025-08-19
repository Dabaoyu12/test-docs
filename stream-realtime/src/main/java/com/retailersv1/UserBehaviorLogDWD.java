package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import com.alibaba.fastjson.JSON;

import java.util.concurrent.TimeUnit;

/**
 * 用户行为日志 ETL + 维度关联
 * 1. 读取 Kafka ODS 日志
 * 2. 多流 join（start、page、display）
 * 3. 异步 I/O 维度 enrich（HBase dim_ 表）
 * 4. 输出到 Kafka DWD
 */
public class UserBehaviorLogDWD {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        String bootstrapServers = "cdh01:9092,cdh02:9092,cdh03:9092";

        // Kafka Sources
        KafkaSource<String> startSource = KafkaUtils.buildKafkaSource(
                bootstrapServers, "ods_start_log", "user_behavior_group", OffsetsInitializer.earliest()
        );
        KafkaSource<String> pageSource = KafkaUtils.buildKafkaSource(
                bootstrapServers, "ods_page_log", "user_behavior_group", OffsetsInitializer.earliest()
        );
        KafkaSource<String> displaySource = KafkaUtils.buildKafkaSource(
                bootstrapServers, "ods_display_log", "user_behavior_group", OffsetsInitializer.earliest()
        );

        DataStreamSource<String> startStream = env.fromSource(startSource, WatermarkStrategy.noWatermarks(), "start-log");
        DataStreamSource<String> pageStream = env.fromSource(pageSource, WatermarkStrategy.noWatermarks(), "page-log");
        DataStreamSource<String> displayStream = env.fromSource(displaySource, WatermarkStrategy.noWatermarks(), "display-log");

        // JSON 转换
        DataStream<JSONObject> startJson = startStream.map(JSONObject::parseObject);
        DataStream<JSONObject> pageJson = pageStream.map(JSONObject::parseObject);
        DataStream<JSONObject> displayJson = displayStream.map(JSONObject::parseObject);

        // join page + display
        SingleOutputStreamOperator<JSONObject> pageDisplayJoined =
                pageJson
                        .keyBy(json -> json.getJSONObject("common").getString("sid"))
                        .intervalJoin(displayJson.keyBy(json -> json.getJSONObject("common").getString("sid")))
                        .between(Time.seconds(-10), Time.seconds(10))
                        .process(new PageDisplayJoinFunction());

        // join start + (page+display)
        SingleOutputStreamOperator<JSONObject> userBehavior =
                startJson
                        .keyBy(json -> json.getJSONObject("common").getString("sid"))
                        .intervalJoin(pageDisplayJoined.keyBy(json -> json.getJSONObject("common").getString("sid")))
                        .between(Time.seconds(-30), Time.seconds(30))
                        .process(new StartPageDisplayJoinFunction());

        // 异步 I/O 维度关联（HBase dim_）
        SingleOutputStreamOperator<JSONObject> enrichedStream =
                AsyncDataStream.unorderedWait(
                        userBehavior,
                        new HBaseAsyncDimFunction(),
                        60,
                        TimeUnit.SECONDS,
                        200
                );

        // 输出到 Kafka
        String targetTopic = "dwd_user_behavior_log";
        if (!KafkaUtils.kafkaTopicExists(bootstrapServers, targetTopic)) {
            KafkaUtils.createKafkaTopic(bootstrapServers, targetTopic, 6, (short) 1, false);
        }
        KafkaSink<String> sink = KafkaUtils.buildKafkaSink(bootstrapServers, targetTopic);

        enrichedStream
                .map(json -> JSON.toJSONString(json))
                .sinkTo(sink);

        env.execute("DWD User Behavior Log Job");
    }
}
