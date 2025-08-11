package medical_04;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.MyKafkaUtil;

public class dim {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds1 = env.addSource(MyKafkaUtil.getKafkaConsumer("medical_ods"));
        ds1.print();



        env.execute();
    }
}
