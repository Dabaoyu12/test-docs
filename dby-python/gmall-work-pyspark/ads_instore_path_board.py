from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, sum, avg, max as spark_max,
                                   min as spark_min, countDistinct, round)

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Create ADS Layer Tables") \
    .enableHiveSupport() \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .getOrCreate()


dws_user_visit_summary = spark.table("gmall_work.dws_user_visit_summary")


ads_user_visit_overview = dws_user_visit_summary \
    .groupBy(col("stat_date")) \
    .agg(

    countDistinct(col("user_id")).alias("total_users"),
    sum(col("session_count")).alias("total_sessions"),
    sum(col("total_page_views")).alias("total_pv"),

    sum(col("bounce_page_count")).alias("total_bounce_pages"),
    round(
        sum(col("bounce_page_count")) / sum(col("total_page_views")),
        4
    ).alias("overall_bounce_rate"),
    sum(col("order_count")).alias("total_orders"),
    round(
        sum(col("order_count")) / countDistinct(col("user_id")),
        2
    ).alias("orders_per_user"),
    countDistinct(col("device_types")).alias("distinct_device_types"),
    spark_min(col("first_visit_time")).alias("earliest_visit_time"),
    spark_max(col("last_visit_time")).alias("latest_visit_time")
) \
    .withColumn(
    "avg_sessions_per_user",
    round(col("total_sessions") / col("total_users"), 2)
) \
    .withColumn(
    "avg_pv_per_session",
    round(col("total_pv") / col("total_sessions"), 2)
) \
    .select(
    col("stat_date"),
    col("total_users"),
    col("total_sessions"),
    col("avg_sessions_per_user"),
    col("total_pv"),
    col("avg_pv_per_session"),
    col("total_bounce_pages"),
    col("overall_bounce_rate"),
    col("total_orders"),
    col("orders_per_user"),
    col("distinct_device_types"),
    col("earliest_visit_time"),
    col("latest_visit_time")
)

# 写入ADS
ads_user_visit_overview.write.mode("overwrite").saveAsTable("gmall_work.ads_user_visit_overview")
print("成功创建ads_user_visit_overview表")

# 关闭SparkSession
spark.stop()