from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, to_timestamp, year, month, dayofmonth,
                                   hour, when, lit, count, sum, min, max)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ByteType

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Process Page Visit Log from Hive") \
    .enableHiveSupport() \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .getOrCreate()

ods_schema = StructType([
    StructField("log_id", IntegerType(), True),
    StructField("session_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("page_type", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("referer_url", StringType(), True),
    StructField("visit_time", StringType(), True),
    StructField("stay_duration", IntegerType(), True),
    StructField("is_order", ByteType(), True),
    StructField("dt", StringType(), True),
    StructField("ds", StringType(), True)
])


spark.sql("CREATE DATABASE IF NOT EXISTS gmall_work")


try:

    ods_page_visit_log = spark.table("gmall_work.ods_page_visit_log")
    print("成功读取到gmall_work.ods_page_visit_log表")
except Exception as e:
    print(f"表gmall_work.ods_page_visit_log不存在，创建临时测试数据: {e}")


    test_data = [
        (1, "sess_123", "user_456", "mobile", "product", "http://gmall.com/product/1", "http://baidu.com", "2025-07-31 08:00:00", 60, 0, "2025-07-31", "20250731"),
        (2, "sess_123", "user_456", "mobile", "cart", "http://gmall.com/cart", "http://gmall.com/product/1", "2025-07-31 08:05:00", 30, 0, "2025-07-31", "20250731"),
        (3, "sess_123", "user_456", "mobile", "order", "http://gmall.com/order", "http://gmall.com/cart", "2025-07-31 08:10:00", 90, 1, "2025-07-31", "20250731"),
        (4, "sess_789", "user_012", "pc", "home", "http://gmall.com", None, "2025-07-31 09:00:00", 45, 0, "2025-07-31", "20250731")
    ]


    ods_page_visit_log = spark.createDataFrame(test_data, schema=ods_schema)
    ods_page_visit_log.write.mode("overwrite").saveAsTable("gmall_work.ods_page_visit_log")
    print("已创建临时表gmall_work.ods_page_visit_log并插入测试数据")


dim_user_device = ods_page_visit_log.groupBy(
    col("user_id"),
    col("device_type"),
    col("session_id"),
    col("dt")
).agg(
    count(lit(1)).alias("total_visits"),
    sum(col("stay_duration")).alias("total_stay_duration"),
    sum(col("is_order")).alias("total_orders"),
    min(col("visit_time")).alias("first_visit_time"),
    max(col("visit_time")).alias("last_visit_time")
).withColumn(
    "is_new_user",

    when(col("first_visit_time").substr(0, 10) == col("dt"), lit(1)).otherwise(lit(0))
).withColumn(
    "first_visit_ts",
    to_timestamp(col("first_visit_time"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "last_visit_ts",
    to_timestamp(col("last_visit_time"), "yyyy-MM-dd HH:mm:ss")
).select(
    col("user_id"),
    col("device_type"),
    col("session_id"),
    col("total_visits"),
    col("total_stay_duration"),
    col("total_orders"),
    col("is_new_user"),
    col("first_visit_time"),
    col("last_visit_time"),
    col("first_visit_ts"),
    col("last_visit_ts"),
    col("dt").alias("stat_date")
)

dim_user_device.write.mode("overwrite").saveAsTable("gmall_work.dim_user_device")
print("成功创建dim_user_device表")


dwd_page_visit_detail = ods_page_visit_log.withColumn(
    "visit_ts",
    to_timestamp(col("visit_time"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "visit_year",
    year(col("visit_ts"))
).withColumn(
    "visit_month",
    month(col("visit_ts"))
).withColumn(
    "visit_day",
    dayofmonth(col("visit_ts"))
).withColumn(
    "visit_hour",
    hour(col("visit_ts"))
).withColumn(
    "is_bounce_page",
    when(col("stay_duration") < 5, lit(1)).otherwise(lit(0))
).withColumn(
    "is_external_referer",
    when(
        col("referer_url").isNotNull() & ~col("referer_url").contains("gmall.com"),
        lit(1)
    ).otherwise(lit(0))
).select(
    col("log_id"),
    col("session_id"),
    col("user_id"),
    col("device_type"),
    col("page_type"),
    col("page_url"),
    col("referer_url"),
    col("visit_time"),
    col("visit_ts"),
    col("visit_year"),
    col("visit_month"),
    col("visit_day"),
    col("visit_hour"),
    col("stay_duration"),
    col("is_bounce_page"),
    col("is_external_referer"),
    col("is_order"),
    col("dt"),
    col("ds")
)

dwd_page_visit_detail.write.mode("overwrite").saveAsTable("gmall_work.dwd_page_visit_detail")
print("成功创建dwd_page_visit_detail表")

# 关闭SparkSession
spark.stop()
