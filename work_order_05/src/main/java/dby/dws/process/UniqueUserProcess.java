package dby.dws.process;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class UniqueUserProcess extends KeyedProcessFunction<String, JSONObject, JSONObject> {
    private transient ValueState<Boolean> seen;

    @Override
    public void open(Configuration parameters) {
        seen = getRuntimeContext().getState(
                new ValueStateDescriptor<>("seen", Boolean.class));
    }

    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        if (seen.value() == null) {
            seen.update(true);
            JSONObject obj = new JSONObject();
            if (value.containsKey("user_id")) {
                obj.put("uv", 1L);
            } else if (value.containsKey("buyer_id")) {
                obj.put("pay_user", 1L);
            }
            out.collect(obj);
        }
    }
}
