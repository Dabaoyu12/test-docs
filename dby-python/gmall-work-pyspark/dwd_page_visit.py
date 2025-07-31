from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, to_timestamp, date_format,
    year, month, dayofmonth, weekofyear, concat, lit
)

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Process Page Visit Log from Hive") \
    .enableHiveSupport() \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .getOrCreate()

# 从Hive读取数据
print("从Hive表读取数据...")
ods_df = spark.table("gmall_work.ods_page_visit_log")

# 查看表结构和样本数据
print("原始表结构:")
ods_df.printSchema()

print("样本数据:")
ods_df.show(5, truncate=False)

# 数据清洗与转换
print("开始数据处理...")
processed_df = ods_df \
    .withColumn("session_id", when(col("session_id").isNull() | (trim(col("session_id")) == ""),
                               concat(lit("session_"), col("log_id"))).otherwise(col("session_id"))) \
    .withColumn("user_id", when(col("user_id").isNull() | (trim(col("user_id")) == ""), "unknown").otherwise(col("user_id"))) \
    .withColumn("device_type", when(col("device_type").isNull(), "unknown")
                .otherwise(col("device_type"))) \
    .withColumn("page_type", when(col("page_type").isNull(), "other").otherwise(col("page_type"))) \
    .withColumn("stay_duration", when(col("stay_duration").isNull() | (col("stay_duration") < 0), 0)
                .otherwise(col("stay_duration"))) \
    .withColumn("is_order", when(col("is_order").isNull(), 0)
                .when(col("is_order").isin(0, 1), col("is_order"))
                .otherwise(0)) \
    .withColumn("visit_time", to_timestamp(col("visit_time"))) \
    .withColumn("visit_date", date_format(col("visit_time"), "yyyy-MM-dd")) \
    .withColumn("visit_hour", date_format(col("visit_time"), "HH")) \
    .withColumn("visit_year", year(col("visit_time"))) \
    .withColumn("visit_month", month(col("visit_time"))) \
    .withColumn("visit_day", dayofmonth(col("visit_time"))) \
    .withColumn("visit_week", weekofyear(col("visit_time"))) \
    .withColumn("referer_type", when(col("referer_url").isNull() | (trim(col("referer_url")) == ""), "direct")
            .when(col("referer_url").contains("baidu.com"), "baidu")
            .when(col("referer_url").contains("google.com"), "google")
            .when(col("referer_url").contains("bing.com"), "bing")
            .when(col("referer_url").contains("weixin.qq.com"), "weixin")
            .otherwise("other")) \
    .withColumn("page_category",
            when(col("page_type").isin("home", "activity"), "store_page")
            .when(col("page_type") == "product", "product_page")
            .when(col("page_type") == "category", "category_page")
            .otherwise("other_page")) \
    .filter(col("log_id").isNotNull()) \
    .filter(col("visit_time").isNotNull())

# 查看处理后的数据
print("处理后的数据结构:")
processed_df.printSchema()

print("处理后的样本数据:")
processed_df.select(
    "log_id", "session_id", "user_id", "device_type",
    "page_type", "page_category", "visit_time", "stay_duration",
    "is_order", "referer_type"
).show(5, truncate=False)

# 将处理后的数据写入Hive的DWD层（数据仓库明细层）
print("写入处理后的数据到Hive...")
processed_df.write \
    .mode("overwrite") \
    .partitionBy("dt", "visit_year", "visit_month") \
    .saveAsTable("gmall_work.dwd_page_visit_log")

print("数据处理完成!")

# 停止SparkSession
spark.stop()