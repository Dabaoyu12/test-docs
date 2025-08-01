from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, datediff, max as spark_max,
                                   count, sum as spark_sum, when, row_number,
                                   lit, percent_rank)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("RFM Customer Value Model") \
    .enableHiveSupport() \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .getOrCreate()

x
dws_user = spark.table("gmall_work.dws_user_visit_summary")
dwd_page = spark.table("gmall_work.dwd_page_visit_detail")

# 定义分析日期
max_date = dws_user.agg(spark_max("stat_date")).collect()[0][0]
print(f"分析基准日期: {max_date}")

# 3. 计算RFM核心指标
rfm_base = dws_user \
    .groupBy("user_id") \
    .agg(
        datediff(lit(max_date), spark_max("last_visit_time").cast("date")).alias("recency"),
        spark_sum("order_count").alias("frequency"),
        spark_sum("order_count").alias("monetary")
    ) \
    .withColumn("frequency", when(col("frequency").isNull(), 0).otherwise(col("frequency"))) \
    .withColumn("monetary", when(col("monetary").isNull(), 0).otherwise(col("monetary"))) \
    .filter(col("user_id").isNotNull() & (col("user_id") != "unknown"))

# 4. RFM评分计算
# 定义评分窗口
rfm_window_r = Window.orderBy(col("recency"))
rfm_window_f = Window.orderBy(col("frequency").desc())
rfm_window_m = Window.orderBy(col("monetary").desc())

rfm_score = rfm_base \
    .withColumn("r_percentile", percent_rank().over(rfm_window_r)) \
    .withColumn("f_percentile", percent_rank().over(rfm_window_f)) \
    .withColumn("m_percentile", percent_rank().over(rfm_window_m)) \
    .withColumn("r_score",
                when(col("r_percentile") <= 0.2, 5)
                .when(col("r_percentile") <= 0.4, 4)
                .when(col("r_percentile") <= 0.6, 3)
                .when(col("r_percentile") <= 0.8, 2)
                .otherwise(1)
                ) \
    .withColumn("f_score",
                when(col("f_percentile") <= 0.2, 5)
                .when(col("f_percentile") <= 0.4, 4)
                .when(col("f_percentile") <= 0.6, 3)
                .when(col("f_percentile") <= 0.8, 2)
                .otherwise(1)
                ) \
    .withColumn("m_score",
                when(col("m_percentile") <= 0.2, 5)
                .when(col("m_percentile") <= 0.4, 4)
                .when(col("m_percentile") <= 0.6, 3)
                .when(col("m_percentile") <= 0.8, 2)
                .otherwise(1)
                ) \
    .withColumn("rfm_total_score", col("r_score") * 0.4 + col("f_score") * 0.3 + col("m_score") * 0.3)

# 客户价值分层
customer_segment = rfm_score \
    .withColumn("customer_level",
                when((col("r_score") >= 4) & (col("f_score") >= 4) & (col("m_score") >= 4), "高价值客户")
                .when((col("r_score") >= 3) & (col("f_score") >= 3) & (col("m_score") >= 3), "潜力客户")
                .when((col("r_score") >= 3) & (col("f_score") < 3) & (col("m_score") >= 3), "流失预警客户")
                .when((col("r_score") < 3) & (col("f_score") >= 3) & (col("m_score") >= 3), "高频低价值客户")
                .otherwise("低价值客户")
                ) \
    .select(
        "user_id", "recency", "frequency", "monetary",
        "r_score", "f_score", "m_score", "rfm_total_score", "customer_level"
    )


customer_segment.write.mode("overwrite").saveAsTable("gmall_work.ads_customer_rfm_segment")
print("RFM客户价值模型计算完成，结果已保存至ads_customer_rfm_segment表")

spark.stop()