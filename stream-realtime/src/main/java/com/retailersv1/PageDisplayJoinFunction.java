package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class PageDisplayJoinFunction extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
    @Override
    public void processElement(JSONObject left, JSONObject right, Context ctx, Collector<JSONObject> out) {
        JSONObject result = new JSONObject();
        result.putAll(left);
        result.putAll(right);
        out.collect(result);
    }
}
