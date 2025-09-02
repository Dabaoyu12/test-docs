package dby.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DwdAsyncJob {

    public static void main(String[] args) throws Exception {
        // 1. Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Kafka 配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "cdh01:9092");

        // ------------------- 模拟各类 DWD 数据 -------------------
        DataStreamSource<String> orderItemSource = env.fromElements(
                JSON.toJSONString(new OrderItem("item_1001", "sku_5001", "shop_101", 2, "order_1001", "user_1001"))
        );

        DataStreamSource<String> paymentSource = env.fromElements(
                JSON.toJSONString(new Payment("user_1001", "order_1001", 1249.0, 0.0, "SUCCESS", "2025-08-26 19:41:16.942"))
        );

        DataStreamSource<String> refundSource = env.fromElements(
                JSON.toJSONString(new Refund("user_1001", "order_1001", 100.0, "2025-08-27 12:00:00"))
        );

        DataStreamSource<String> cartSource = env.fromElements(
                JSON.toJSONString(new Cart("user_1001", "item_1001", "sku_5001", "shop_101", 3))
        );

        DataStreamSource<String> userSource = env.fromElements(
                JSON.toJSONString(new User("user_1001", "男", 23, "山东", "iOS", "2025-08-15 12:37:21"))
        );

        DataStreamSource<String> itemSource = env.fromElements(
                JSON.toJSONString(new Item("item_1001", "sku_5001", "shop_101", "cat_10", "商品A", 599.0))
        );

        DataStreamSource<String> shopSource = env.fromElements(
                JSON.toJSONString(new Shop("shop_101", "店铺A", "seller_101", "cat_10"))
        );

        DataStreamSource<String> skuSource = env.fromElements(
                JSON.toJSONString(new SKU("sku_5001", "item_1001", "shop_101", 599.0))
        );

        DataStreamSource<String> dateSource = env.fromElements(
                JSON.toJSONString(new DateDim("20250826", 2025, 8, 26))
        );

        DataStreamSource<String> categorySource = env.fromElements(
                JSON.toJSONString(new Category("cat_10", "类目_10"))
        );

        // ------------------- 输出到 Kafka -------------------
        sendToKafka(orderItemSource, "dwd_order_item_05", props);
        sendToKafka(paymentSource, "dwd_payment_05", props);
        sendToKafka(refundSource, "dwd_refund_05", props);
        sendToKafka(cartSource, "dwd_cart_05", props);
        sendToKafka(userSource, "dwd_user_05", props);
        sendToKafka(itemSource, "dwd_item_05", props);
        sendToKafka(shopSource, "dwd_shop_05", props);
        sendToKafka(skuSource, "dwd_sku_05", props);
        sendToKafka(dateSource, "dwd_date_05", props);
        sendToKafka(categorySource, "dwd_category_05", props);

        env.execute("DWD All Tables To Kafka");
    }

    private static void sendToKafka(DataStreamSource<String> source, String topic, Properties props) {
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(
                topic,
                (KafkaSerializationSchema<String>) (element, timestamp) ->
                        new ProducerRecord<>(topic, element.getBytes()),
                props,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
        source.addSink(kafkaSink);
    }

    // ------------------- 模拟 POJO -------------------
    public static class OrderItem { public String item_id, sku_id, shop_id, order_id, user_id; public int quantity;
        public OrderItem(String item_id, String sku_id, String shop_id, int quantity, String order_id, String user_id){
            this.item_id=item_id;this.sku_id=sku_id;this.shop_id=shop_id;this.quantity=quantity;this.order_id=order_id;this.user_id=user_id;
        }
    }
    public static class Payment { public String buyer_id, order_id, pay_status, pay_time; public Double pay_amount, refund_amount;
        public Payment(String buyer_id, String order_id, Double pay_amount, Double refund_amount, String pay_status, String pay_time){
            this.buyer_id=buyer_id;this.order_id=order_id;this.pay_amount=pay_amount;this.refund_amount=refund_amount;this.pay_status=pay_status;this.pay_time=pay_time;
        }
    }
    public static class Refund { public String buyer_id, order_id, refund_time; public Double refund_amount;
        public Refund(String buyer_id, String order_id, Double refund_amount, String refund_time){ this.buyer_id=buyer_id; this.order_id=order_id; this.refund_amount=refund_amount; this.refund_time=refund_time;}
    }
    public static class Cart { public String user_id, item_id, sku_id, shop_id; public int quantity;
        public Cart(String user_id, String item_id, String sku_id, String shop_id, int quantity){ this.user_id=user_id; this.item_id=item_id; this.sku_id=sku_id; this.shop_id=shop_id; this.quantity=quantity;}
    }
    public static class User { public String user_id, gender, region, device_type, register_time; public int age;
        public User(String user_id, String gender, int age, String region, String device_type, String register_time){ this.user_id=user_id; this.gender=gender; this.age=age; this.region=region; this.device_type=device_type; this.register_time=register_time;}
    }
    public static class Item { public String item_id, sku_id, shop_id, category_id, item_name; public double price;
        public Item(String item_id, String sku_id, String shop_id, String category_id, String item_name, double price){ this.item_id=item_id;this.sku_id=sku_id;this.shop_id=shop_id;this.category_id=category_id;this.item_name=item_name;this.price=price;}
    }
    public static class Shop { public String shop_id, shop_name, seller_id, category_id;
        public Shop(String shop_id, String shop_name, String seller_id, String category_id){ this.shop_id=shop_id;this.shop_name=shop_name;this.seller_id=seller_id;this.category_id=category_id;}
    }
    public static class SKU { public String sku_id, item_id, shop_id; public double price;
        public SKU(String sku_id, String item_id, String shop_id, double price){ this.sku_id=sku_id;this.item_id=item_id;this.shop_id=shop_id;this.price=price;}
    }
    public static class DateDim { public String date_id; public int year, month, day;
        public DateDim(String date_id, int year, int month, int day){ this.date_id=date_id;this.year=year;this.month=month;this.day=day;}
    }
    public static class Category { public String category_id, category_name;
        public Category(String category_id, String category_name){ this.category_id=category_id;this.category_name=category_name;}
    }
}
