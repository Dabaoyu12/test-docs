from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, concat_ws, collect_list, count,
                                   countDistinct, sum, avg, round, lit,
                                   row_number, split, size, first, last,
                                   to_timestamp, expr, explode, map_from_entries, struct)
from pyspark.sql.window import Window

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Instore Path Board - Non-layered") \
    .enableHiveSupport() \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# 1. 读取ODS层数据并进行必要的清洗
print("读取ODS层数据...")
ods_df = spark.table("gmall_work.ods_page_visit_log") \
    .filter(col("log_id").isNotNull()) \
    .filter(col("session_id").isNotNull()) \
    .filter(col("visit_time").isNotNull()) \
    .withColumn("visit_ts", to_timestamp(col("visit_time"), "yyyy-MM-dd HH:mm:ss"))

# 2. 为每个会话的页面访问添加序号，用于生成路径
print("生成页面访问序列...")
window_spec = Window.partitionBy("session_id").orderBy("visit_ts")
session_page_sequence = ods_df \
    .withColumn("page_order", row_number().over(window_spec)) \
    .select(
    col("dt").alias("stat_date"),
    col("session_id"),
    col("user_id"),
    col("page_type"),
    col("page_order"),
    col("visit_ts"),
    col("stay_duration"),
    col("is_order")
)

# 3. 按会话聚合，生成页面访问路径
print("聚合生成访问路径...")
session_path_df = session_page_sequence \
    .groupBy("stat_date", "session_id", "user_id") \
    .agg(
    collect_list(col("page_type")).alias("page_path_list"),  # 页面类型路径
    collect_list(col("page_order")).alias("order_list"),     # 页面顺序
    sum(col("stay_duration")).alias("total_stay_time"),      # 会话总停留时间
    sum(col("is_order")).alias("order_count"),               # 会话下单次数
    count(col("page_type")).alias("path_length"),            # 路径长度
    first(col("page_type")).alias("entry_page"),             # 入口页面
    last(col("page_type")).alias("exit_page")                # 出口页面
) \
    .withColumn("page_path", concat_ws("->", col("page_path_list")))  # 路径字符串

# 4. 计算路径相关指标
print("计算路径指标...")
path_analysis_df = session_path_df \
    .groupBy("stat_date") \
    .agg(
    # 基础指标
    countDistinct("user_id").alias("total_users"),
    countDistinct("session_id").alias("total_sessions"),
    avg("path_length").alias("avg_path_length"),
    avg("total_stay_time").alias("avg_stay_time_per_path"),

    # 路径转化指标
    sum("order_count").alias("total_orders"),
    round(
        sum("order_count") / countDistinct("session_id"),
        4
    ).alias("conversion_rate_to_order"),

    # 热门入口和出口
    first("entry_page").alias("most_popular_entry"),
    first("exit_page").alias("most_popular_exit")
)

# 5. 计算页面路径出现次数
print("计算页面路径频次...")
path_frequency_df = session_path_df \
    .groupBy("stat_date", "page_path") \
    .agg(count("session_id").alias("path_count"))

# 6. 计算页面间转化率
print("计算页面转化率...")
# 提取路径中的页面对
page_transition_df = session_path_df \
    .filter(size(col("page_path_list")) > 1) \
    .select(
    col("stat_date"),
    col("page_path_list")
) \
    .withColumn("transitions", expr(
    "transform(sequence(0, size(page_path_list)-2), " +
    "i -> concat(page_path_list[i], '->', page_path_list[i+1]))"
)) \
    .select(
    col("stat_date"),
    explode(col("transitions")).alias("page_transition")
) \
    .groupBy("stat_date", "page_transition") \
    .agg(count("*").alias("transition_count"))

# 7. 关联所有指标得到最终结果
print("合并所有指标...")
final_df = path_analysis_df \
    .join(
    path_frequency_df.groupBy("stat_date")
    .agg(map_from_entries(collect_list(struct("page_path", "path_count")))
         .alias("page_path_count")),
    on="stat_date",
    how="left"
) \
    .join(
    page_transition_df.groupBy("stat_date")
    .agg(map_from_entries(collect_list(struct("page_transition", "transition_count")))
         .alias("page_transition_count")),
    on="stat_date",
    how="left"
)

# 8. 计算页面转化率并选择最终字段
result_df = final_df \
    .select(
    col("stat_date"),
    col("total_users"),
    col("total_sessions"),
    col("page_path_count"),
    col("page_transition_count").alias("page_conversion_rate"),
    col("avg_path_length"),
    col("most_popular_entry"),
    col("most_popular_exit"),
    col("avg_stay_time_per_path"),
    col("conversion_rate_to_order")
)

# 9. 写入ADS层表
print("写入ADS层表...")
result_df.write.mode("overwrite").saveAsTable("gmall_work.ads_instore_path_summary")

print("不分层方式实现的店内路径看板数据生成完成！")

# 关闭SparkSession
spark.stop()