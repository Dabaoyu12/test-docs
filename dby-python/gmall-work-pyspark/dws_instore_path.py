from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, countDistinct, sum, avg, max as spark_max,
                                   min as spark_min, concat_ws, collect_set, count)

# 初始化SparkSession（复用已有配置）
spark = SparkSession.builder \
    .appName("Create DWS Layer Tables") \
    .enableHiveSupport() \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .getOrCreate()

# 读取已创建的维度表和明细宽表
dim_user_device = spark.table("gmall_work.dim_user_device")
dwd_page_visit_detail = spark.table("gmall_work.dwd_page_visit_detail")

# 创建DWS层：用户访问汇总表（按用户+日期聚合）
dws_user_visit_summary = dwd_page_visit_detail \
    .join(  # 关联用户设备维度表
        dim_user_device,
        [
            dwd_page_visit_detail.user_id == dim_user_device.user_id,
            dwd_page_visit_detail.session_id == dim_user_device.session_id,
            dwd_page_visit_detail.dt == dim_user_device.stat_date
        ],
        "left"
    ) \
    .groupBy(  # 按用户和日期分组聚合
        dwd_page_visit_detail.user_id,
        dwd_page_visit_detail.dt
    ) \
    .agg(  # 访问行为指标
        countDistinct(dwd_page_visit_detail.session_id).alias("session_count"),
        count(dwd_page_visit_detail.log_id).alias("total_page_views"),
        sum(dwd_page_visit_detail.stay_duration).alias("total_stay_time"),
        avg(dwd_page_visit_detail.stay_duration).alias("avg_stay_time_per_page"),
        sum(dwd_page_visit_detail.is_bounce_page).alias("bounce_page_count"),
        sum(dwd_page_visit_detail.is_order).alias("order_count"),
        # 设备相关指标（来自维度表）
        countDistinct(dim_user_device.device_type).alias("device_type_count"),
        concat_ws("|", collect_set(dim_user_device.device_type)).alias("device_types"),
        # 时间相关指标
        spark_min(dwd_page_visit_detail.visit_time).alias("first_visit_time"),
        spark_max(dwd_page_visit_detail.visit_time).alias("last_visit_time")
    ) \
    .withColumn(  # 衍生指标计算
        "bounce_rate",
        col("bounce_page_count") / col("total_page_views")
    ) \
    .withColumn(
        "avg_page_views_per_session",
        col("total_page_views") / col("session_count")
    ) \
    .select(
        col("user_id"),
        col("dt").alias("stat_date"),
        col("session_count"),
        col("total_page_views"),
        col("avg_page_views_per_session"),
        col("total_stay_time"),
        col("avg_stay_time_per_page"),
        col("bounce_page_count"),
        col("bounce_rate"),
        col("order_count"),
        col("device_type_count"),
        col("device_types"),
        col("first_visit_time"),
        col("last_visit_time")
    )

# 写入DWS层表
dws_user_visit_summary.write.mode("overwrite").saveAsTable("gmall_work.dws_user_visit_summary")
print("成功创建dws_user_visit_summary表")

# 关闭SparkSession
spark.stop()