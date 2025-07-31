from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_date, lit, to_timestamp, rand, concat

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Create Dim Page Visit Log") \
    .enableHiveSupport() \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .getOrCreate()


# 先检查数据库是否存在，如果不存在则创建
spark.sql("CREATE DATABASE IF NOT EXISTS gmall_work")

# 检查源表是否存在
try:
    # 尝试读取表
    spark.table("gmall_work.ods_page_visit_log")
    print("源表存在，可以正常读取")
except:
    print("源表不存在，创建测试表并插入测试数据")
    # 创建测试表


# 之后继续原来的处理逻辑...
source_df = spark.table("gmall_work.ods_page_visit_log")


# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Create Dim Page Visit Log") \
    .enableHiveSupport() \
    .getOrCreate()


source_df = spark.table("gmall_work.ods_page_visit_log")

dim_page_visit_log = source_df \
.select(
    col("log_id"),
    col("session_id"),
    col("user_id"),
    when(col("device_type").isNull(), "unknown").otherwise(col("device_type")).alias("device_type"),
    when(col("page_type").isNull(), "other").otherwise(col("page_type")).alias("page_type"),
    col("page_url"),
    when(col("referer_url").isNull() | (col("referer_url") == ""), "direct").otherwise(col("referer_url")).alias("referer_url"),
    to_timestamp(col("visit_time"), "yyyy-MM-dd HH:mm:ss").alias("visit_time"),
    when((col("stay_duration").isNull()) | (col("stay_duration") < 0), 0).otherwise(col("stay_duration")).alias("stay_duration"),
    when(col("is_order").isNull(), 0).otherwise(col("is_order")).alias("is_order"),
    col("dt"),
    col("ds")
) \
.filter(col("log_id").isNotNull()) \
.withColumn("load_time", current_date())


filled_dim_df = dim_page_visit_log \
.withColumn("user_id", when(col("user_id").isNull(), concat(lit("user_"), rand().cast("string").substr(3, 8))).otherwise(col("user_id"))) \
.withColumn("session_id", when(col("session_id").isNull(), concat(lit("session_"), rand().cast("string").substr(3, 10))).otherwise(col("session_id")))


filled_dim_df.write \
    .mode("overwrite") \
    .partitionBy("dt", "ds") \
    .saveAsTable("gmall_work.dim_page_visit_log")

print("Dim层表创建完成，数据已填充")

# 停止SparkSession
spark.stop()