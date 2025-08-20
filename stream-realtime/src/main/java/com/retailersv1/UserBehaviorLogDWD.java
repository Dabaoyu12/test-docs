package com.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.util.*;

/**
 * 用户行为日志 DWD 任务
 * - 读取 Kafka ODS：ods_start_log / ods_page_log / ods_display_log / ods_action_log
 * - 以 common.sid 为 key 做三层 intervalJoin：
 *   1) page + display
 *   2) page_display + action
 *   3) start + (page_display_action)
 * - 异步 I/O 关联 HBase 的维度表 dim_*
 * - 输出到 Kafka：dwd_user_behavior_log
 */
public class UserBehaviorLogDWD {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        final String bootstrapServers = "cdh01:9092,cdh02:9092,cdh03:9092";

        // Kafka sources
        KafkaSource<String> startSource = KafkaUtils.buildKafkaSource(
                bootstrapServers, "ods_start_log", "user_behavior_group", OffsetsInitializer.earliest()
        );
        KafkaSource<String> pageSource = KafkaUtils.buildKafkaSource(
                bootstrapServers, "ods_page_log", "user_behavior_group", OffsetsInitializer.earliest()
        );
        KafkaSource<String> displaySource = KafkaUtils.buildKafkaSource(
                bootstrapServers, "ods_display_log", "user_behavior_group", OffsetsInitializer.earliest()
        );
        KafkaSource<String> actionSource = KafkaUtils.buildKafkaSource(
                bootstrapServers, "ods_action_log", "user_behavior_group", OffsetsInitializer.earliest()
        );

        DataStreamSource<String> startRaw = env.fromSource(startSource, WatermarkStrategy.noWatermarks(), "ods_start_log");
        DataStreamSource<String> pageRaw = env.fromSource(pageSource, WatermarkStrategy.noWatermarks(), "ods_page_log");
        DataStreamSource<String> displayRaw = env.fromSource(displaySource, WatermarkStrategy.noWatermarks(), "ods_display_log");
        DataStreamSource<String> actionRaw = env.fromSource(actionSource, WatermarkStrategy.noWatermarks(), "ods_action_log");

        DataStream<JSONObject> startJson = startRaw
                .map(UserBehaviorLogDWD::safeParse)
                .filter(Objects::nonNull)
                .map(json -> normalize(json, "start"))
                .filter(json -> json.getString("sid") != null);

        DataStream<JSONObject> pageJson = pageRaw
                .map(UserBehaviorLogDWD::safeParse)
                .filter(Objects::nonNull)
                .map(json -> normalize(json, "page"))
                .filter(json -> json.getString("sid") != null);

        DataStream<JSONObject> displayJson = displayRaw
                .map(UserBehaviorLogDWD::safeParse)
                .filter(Objects::nonNull)
                .map(json -> normalize(json, "display"))
                .filter(json -> json.getString("sid") != null);

        DataStream<JSONObject> actionJson = actionRaw
                .map(UserBehaviorLogDWD::safeParse)
                .filter(Objects::nonNull)
                .map(json -> normalize(json, "action"))
                .filter(json -> json.getString("sid") != null);

        // 1) page + display
        SingleOutputStreamOperator<JSONObject> pageDisplayJoined =
                pageJson.keyBy(json -> json.getString("sid"))
                        .intervalJoin(displayJson.keyBy(json -> json.getString("sid")))
                        .between(Time.milliseconds(-10_000), Time.milliseconds(10_000))
                        .process(new PageDisplayJoinFunction());

        // 2) page_display + action
        SingleOutputStreamOperator<JSONObject> pageDisplayActionJoined =
                pageDisplayJoined.keyBy(json -> json.getString("sid"))
                        .intervalJoin(actionJson.keyBy(json -> json.getString("sid")))
                        .between(Time.milliseconds(-10_000), Time.milliseconds(10_000))
                        .process(new PageDisplayActionJoinFunction());

        // 3) start + page_display_action
        SingleOutputStreamOperator<JSONObject> userBehavior =
                startJson.keyBy(json -> json.getString("sid"))
                        .intervalJoin(pageDisplayActionJoined.keyBy(json -> json.getString("sid")))
                        .between(Time.milliseconds(-30_000), Time.milliseconds(30_000))
                        .process(new StartPageDisplayActionJoinFunction());

        // Kafka Sink
        final String targetTopic = "dwd_user_behavior_log";
        if (!KafkaUtils.kafkaTopicExists(bootstrapServers, targetTopic)) {
            KafkaUtils.createKafkaTopic(bootstrapServers, targetTopic, 6, (short) 1, false);
        }
        KafkaSink<String> sink = KafkaUtils.buildKafkaSink(bootstrapServers, targetTopic);



        userBehavior
                .map(obj -> obj.toJSONString())
                .sinkTo(sink);


        env.execute("DWD User Behavior Log Job");
    }

    private static JSONObject safeParse(String line) {
        try {
            if (line == null || line.trim().isEmpty()) return null;
            return JSON.parseObject(line);
        } catch (Exception e) {
            return null;
        }
    }

    private static JSONObject normalize(JSONObject raw, String eventType) {
        if (raw == null) return null;
        JSONObject out = new JSONObject(true);
        JSONObject common = raw.getJSONObject("common");
        if (common != null) {
            for (String k : Arrays.asList("sid","uid","mid","ch","os","vc","ar","md","ba")) {
                if (common.containsKey(k)) out.put(k, common.getString(k));
            }
        }
        out.put("event_type", eventType);
        out.put("ts", raw.getLongValue("ts"));

        JSONObject detail = new JSONObject(true);
        if ("start".equals(eventType)) {
            JSONObject s = raw.getJSONObject("start");
            if (s != null) detail.putAll(s);
        } else if ("page".equals(eventType)) {
            JSONObject p = raw.getJSONObject("page");
            if (p != null) detail.put("page", p);
        } else if ("display".equals(eventType)) {
            JSONArray displays = raw.getJSONArray("displays");
            if (displays != null) detail.put("displays", displays);
        } else if ("action".equals(eventType)) {
            JSONObject a = raw.getJSONObject("action");
            if (a != null) detail.putAll(a);
        }
        out.put("event_detail", detail);
        return out;
    }

    // Join1
    public static class PageDisplayJoinFunction extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
        @Override
        public void processElement(JSONObject page, JSONObject display, Context ctx, Collector<JSONObject> out) {
            JSONObject result = JSON.parseObject(page.toJSONString());
            JSONObject detail = result.getJSONObject("event_detail");
            if (detail == null) detail = new JSONObject(true);

            JSONObject displayDetail = display.getJSONObject("event_detail");
            if (displayDetail != null && displayDetail.containsKey("displays")) {
                detail.put("displays", displayDetail.getJSONArray("displays"));
            }
            result.put("event_detail", detail);
            result.put("event_type", "page_display");
            out.collect(result);
        }
    }

    // Join2
    public static class PageDisplayActionJoinFunction extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
        @Override
        public void processElement(JSONObject pageDisplay, JSONObject action, Context ctx, Collector<JSONObject> out) {
            JSONObject result = JSON.parseObject(pageDisplay.toJSONString());
            JSONObject detail = result.getJSONObject("event_detail");
            if (detail == null) detail = new JSONObject(true);

            JSONObject actionDetail = action.getJSONObject("event_detail");
            if (actionDetail != null && !actionDetail.isEmpty()) {
                detail.put("action", actionDetail);
            } else {
                // 占位：即使没有 action，也放个空对象
                detail.put("action", new JSONObject());
            }
            result.put("event_detail", detail);
            result.put("event_type", "page_display_action");
            out.collect(result);
        }
    }

    // Join3
    public static class StartPageDisplayActionJoinFunction extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
        @Override
        public void processElement(JSONObject start, JSONObject pageDisplayAction, Context ctx, Collector<JSONObject> out) {
            JSONObject result = JSON.parseObject(pageDisplayAction.toJSONString());
            JSONObject detail = result.getJSONObject("event_detail");
            if (detail == null) detail = new JSONObject(true);

            JSONObject startDetail = start.getJSONObject("event_detail");
            if (startDetail != null) {
                detail.put("start", startDetail);
            }
            // 如果没有 action，也保证有 action 占位
            if (!detail.containsKey("action")) {
                detail.put("action", new JSONObject());
            }
            result.put("event_detail", detail);
            result.put("event_type", "start_page_display_action");
            out.collect(result);
        }
    }
}
