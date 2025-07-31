from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, sum, avg, max as spark_max,
                                   min as spark_min, countDistinct, round)

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Create ADS Layer Tables") \
    .enableHiveSupport() \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .getOrCreate()

# 读取DWS层用户访问汇总表
dws_user_visit_summary = spark.table("gmall_work.dws_user_visit_summary")

# 创建ADS层：用户访问总览分析表（按日期聚合）
ads_user_visit_overview = dws_user_visit_summary \
    .groupBy(col("stat_date")) \
    .agg(
    # 核心访问指标
    countDistinct(col("user_id")).alias("total_users"),  # 总独立用户数
    sum(col("session_count")).alias("total_sessions"),   # 总会话数
    sum(col("total_page_views")).alias("total_pv"),      # 总页面浏览量
    # 行为质量指标
    sum(col("bounce_page_count")).alias("total_bounce_pages"),  # 总跳出页面数
    round(
        sum(col("bounce_page_count")) / sum(col("total_page_views")),
        4
    ).alias("overall_bounce_rate"),  # 整体跳出率
    # 转化指标
    sum(col("order_count")).alias("total_orders"),  # 总订单数
    round(
        sum(col("order_count")) / countDistinct(col("user_id")),
        2
    ).alias("orders_per_user"),  # 人均订单数
    # 设备指标
    countDistinct(col("device_types")).alias("distinct_device_types"),  # 不同设备类型总数
    # 时间指标
    spark_min(col("first_visit_time")).alias("earliest_visit_time"),  # 当日最早访问时间
    spark_max(col("last_visit_time")).alias("latest_visit_time")      # 当日最晚访问时间
) \
    .withColumn(
    "avg_sessions_per_user",  # 人均会话数
    round(col("total_sessions") / col("total_users"), 2)
) \
    .withColumn(
    "avg_pv_per_session",  # 每会话平均浏览量
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

# 写入ADS层表
ads_user_visit_overview.write.mode("overwrite").saveAsTable("gmall_work.ads_user_visit_overview")
print("成功创建ads_user_visit_overview表")

# 关闭SparkSession
spark.stop()