package medical_04;


import util.FlinkCDC;
import util.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ods {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
   //     env.setStateBackend(new FsStateBackend("hdfs://cdh01:8020/medical_test01"));
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/medical_test01");

        SingleOutputStreamOperator<String> ds = FlinkCDC.mysqlCDC(env, "medical", "*");

        ds.print();

        ds.addSink(MyKafkaUtil.getKafkaProducer("medical_ods"));

        env.execute("ods");
    }

}