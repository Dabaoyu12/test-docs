# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit, sum, coalesce, when, col, round, expr, avg, count, desc, asc
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DecimalType, IntegerType, TinyType
)
import sys


def get_spark_session():
    """获取配置好的 SparkSession"""
    spark = SparkSession.builder \
        .appName("TMSAdsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    spark.sql("USE tms")  # 切换到目标数据库
    return spark


def insert_to_hive(df, table_name, partition_date):
    """将 DataFrame 写入 Hive 表，兼容低版本 Python"""
    if df.rdd.isEmpty():
        print(f"[WARN] No data found for {table_name} on {partition_date}, skipping write.")
        return
    # 写入 Hive 表（覆盖模式）
    df.write.mode("overwrite").insertInto(f"tms.{table_name}")


def etl_ads_city_stats(spark, ds):
    """城市分析表（DSL实现）"""
    # 1. 定义表结构
    ads_city_stats_schema = StructType([
        StructField("ds", StringType(), comment="统计日期"),
        StructField("recent_days", LongType(), comment="最近天数，1:最近1天，7:最近7天，30:最近30天"),
        StructField("city_id", LongType(), comment="城市ID"),
        StructField("city_name", StringType(), comment="城市名称"),
        StructField("order_count", LongType(), comment="下单数"),
        StructField("order_amount", DecimalType(), comment="下单金额"),
        StructField("trans_finish_count", LongType(), comment="完成运输次数"),
        StructField("trans_finish_distance", DecimalType(16, 2), comment="完成运输里程"),
        StructField("trans_finish_dur_sec", LongType(), comment="完成运输时长（秒）"),
        StructField("avg_trans_finish_distance", DecimalType(16, 2), comment="平均每次运输里程"),
        StructField("avg_trans_finish_dur_sec", LongType(), comment="平均每次运输时长（秒）")
    ])

    # 2. 删除旧表并创建新表
    spark.sql("drop table if exists ads_city_stats")
    # 用空DataFrame创建表结构（外部表）
    spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=ads_city_stats_schema) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("sep", "\t") \
        .option("path", "/warehouse/tms/ads/ads_city_stats") \
        .option("comment", "城市分析") \
        .saveAsTable("tms.ads_city_stats")

    # 3. 构建数据（最近1天数据）
    # 3.1 订单数据（dws_trade_org_cargo_type_order_1d）
    city_order_1d = spark.table("dws_trade_org_cargo_type_order_1d") \
        .filter(col("ds") == ds) \
        .groupBy("city_id", "city_name") \
        .agg(
        sum("order_count").alias("order_count"),
        sum("order_amount").alias("order_amount")
    ) \
        .withColumn("ds", lit(ds)) \
        .withColumn("recent_days", lit(1))

    # 3.2 运输数据（dws_trans_org_truck_model_type_trans_finish_1d关联维度表）
    # 3.2.1 基础运输数据
    trans_origin = spark.table("dws_trans_org_truck_model_type_trans_finish_1d") \
        .filter(col("ds") == ds) \
        .select("org_id", "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec")

    # 3.2.2 关联机构维度表
    organ = spark.table("dim_organ_full") \
        .filter(col("ds") == ds) \
        .select("id", "org_level", "region_id")

    # 3.2.3 关联区域维度表（城市层级）
    city_level1 = spark.table("dim_region_full") \
        .filter(col("ds") == ds) \
        .select("id", "name", "parent_id") \
        .withColumnRenamed("id", "level1_id") \
        .withColumnRenamed("name", "level1_name") \
        .withColumnRenamed("parent_id", "level1_parent_id")

    city_level2 = spark.table("dim_region_full") \
        .filter(col("ds") == ds) \
        .select("id", "name") \
        .withColumnRenamed("id", "level2_id") \
        .withColumnRenamed("name", "level2_name")

    # 3.2.4 多表关联计算城市ID/名称
    trans_city = trans_origin \
        .join(organ, trans_origin["org_id"] == organ["id"], "left") \
        .join(city_level1, organ["region_id"] == city_level1["level1_id"], "left") \
        .join(city_level2, city_level1["level1_parent_id"] == city_level2["level2_id"], "left") \
        .withColumn("city_id", when(col("org_level") == 1, col("level1_id")).otherwise(col("level2_id"))) \
        .withColumn("city_name", when(col("org_level") == 1, col("level1_name")).otherwise(col("level2_name"))) \
        .select("city_id", "city_name", "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec")

    # 3.2.5 聚合运输数据
    city_trans_1d = trans_city \
        .groupBy("city_id", "city_name") \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum(coalesce("trans_finish_dur_sec", lit(0))).alias("trans_finish_dur_sec"),
        when(
            sum("trans_finish_count") > 0,
            sum("trans_finish_distance") / sum("trans_finish_count")
        ).otherwise(lit(0)).alias("avg_trans_finish_distance"),
        when(
            sum("trans_finish_count") > 0,
            sum(coalesce("trans_finish_dur_sec", lit(0))) / sum("trans_finish_count")
        ).otherwise(lit(0)).alias("avg_trans_finish_dur_sec")
    ) \
        .withColumn("ds", lit(ds)) \
        .withColumn("recent_days", lit(1))

    # 3.3 关联订单和运输数据（最近1天）
    day1_data = city_order_1d.join(
        city_trans_1d,
        on=["ds", "recent_days", "city_id", "city_name"],
        how="full_outer"
    ).select(
        coalesce(city_order_1d["ds"], city_trans_1d["ds"]).alias("ds"),
        coalesce(city_order_1d["recent_days"], city_trans_1d["recent_days"]).alias("recent_days"),
        coalesce(city_order_1d["city_id"], city_trans_1d["city_id"]).alias("city_id"),
        coalesce(city_order_1d["city_name"], city_trans_1d["city_name"]).alias("city_name"),
        col("order_count"),
        col("order_amount"),
        col("trans_finish_count"),
        col("trans_finish_distance"),
        coalesce(col("trans_finish_dur_sec"), lit(0)).alias("trans_finish_dur_sec"),
        coalesce(col("avg_trans_finish_distance"), lit(0)).alias("avg_trans_finish_distance"),
        coalesce(col("avg_trans_finish_dur_sec"), lit(0)).alias("avg_trans_finish_dur_sec")
    )

    # 4. 构建数据（最近7/30天数据）
    # 4.1 订单数据（dws_trade_org_cargo_type_order_nd）
    city_order_nd = spark.table("dws_trade_org_cargo_type_order_nd") \
        .filter(col("ds") == ds) \
        .groupBy("city_id", "city_name", "recent_days") \
        .agg(
        sum("order_count").alias("order_count"),
        sum("order_amount").alias("order_amount")
    ) \
        .withColumn("ds", lit(ds))

    # 4.2 运输数据（dws_trans_shift_trans_finish_nd）
    city_trans_nd = spark.table("dws_trans_shift_trans_finish_nd") \
        .filter(col("ds") == ds) \
        .groupBy("city_id", "city_name", "recent_days") \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum(coalesce("trans_finish_dur_sec", lit(0))).alias("trans_finish_dur_sec"),
        when(
            sum("trans_finish_count") > 0,
            sum("trans_finish_distance") / sum("trans_finish_count")
        ).otherwise(lit(0)).alias("avg_trans_finish_distance"),
        when(
            sum("trans_finish_count") > 0,
            sum(coalesce("trans_finish_dur_sec", lit(0))) / sum("trans_finish_count")
        ).otherwise(lit(0)).alias("avg_trans_finish_dur_sec")
    ) \
        .withColumn("ds", lit(ds))

    # 4.3 关联订单和运输数据（最近7/30天）
    nd_data = city_order_nd.join(
        city_trans_nd,
        on=["ds", "recent_days", "city_id", "city_name"],
        how="full_outer"
    ).select(
        coalesce(city_order_nd["ds"], city_trans_nd["ds"]).alias("ds"),
        coalesce(city_order_nd["recent_days"], city_trans_nd["recent_days"]).alias("recent_days"),
        coalesce(city_order_nd["city_id"], city_trans_nd["city_id"]).alias("city_id"),
        coalesce(city_order_nd["city_name"], city_trans_nd["city_name"]).alias("city_name"),
        col("order_count"),
        col("order_amount"),
        col("trans_finish_count"),
        col("trans_finish_distance"),
        coalesce(col("trans_finish_dur_sec"), lit(0)).alias("trans_finish_dur_sec"),
        coalesce(col("avg_trans_finish_distance"), lit(0)).alias("avg_trans_finish_distance"),
        coalesce(col("avg_trans_finish_dur_sec"), lit(0)).alias("avg_trans_finish_dur_sec")
    )

    # 5. 合并数据并写入
    final_data = day1_data.unionByName(nd_data)
    insert_to_hive(final_data, "ads_city_stats", ds)


def etl_ads_driver_stats(spark, ds):
    """司机分析表（DSL实现）"""
    # 1. 定义表结构
    ads_driver_stats_schema = StructType([
        StructField("ds", StringType(), comment="统计日期"),
        StructField("recent_days", IntegerType(), comment="最近天数（7/30天）"),
        StructField("driver_emp_id", LongType(), comment="第一司机员工ID"),
        StructField("driver_name", StringType(), comment="第一司机姓名"),
        StructField("trans_finish_count", LongType(), comment="完成运输次数"),
        StructField("trans_finish_distance", DecimalType(16, 2), comment="完成运输里程"),
        StructField("trans_finish_dur_sec", LongType(), comment="完成运输时长（秒）"),
        StructField("avg_trans_finish_distance", DecimalType(16, 2), comment="平均每次运输里程"),
        StructField("avg_trans_finish_dur_sec", LongType(), comment="平均每次运输时长（秒）"),
        StructField("trans_finish_late_count", LongType(), comment="逾期次数")
    ])

    # 2. 删除旧表并创建新表
    spark.sql("drop table if exists ads_driver_stats")
    spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=ads_driver_stats_schema) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("sep", "\t") \
        .option("path", "/warehouse/tms/ads/ads_driver_stats") \
        .option("comment", "司机分析") \
        .saveAsTable("tms.ads_driver_stats")

    # 3. 构建数据（单司机数据）
    single_driver = spark.table("dws_trans_shift_trans_finish_nd") \
        .filter((col("ds") == ds) & (col("driver2_emp_id").isNull()) & (col("recent_days").isin(7, 30))) \
        .select(
        "recent_days",
        col("driver1_emp_id").alias("driver_id"),
        col("driver1_name").alias("driver_name"),
        "trans_finish_count",
        "trans_finish_distance",
        "trans_finish_dur_sec",
        "trans_finish_delay_count"
    )

    # 4. 构建数据（双司机数据，拆分里程和时长）
    double_driver = spark.table("dws_trans_shift_trans_finish_nd") \
        .filter((col("ds") == ds) & (col("driver2_emp_id").isNotNull()) & (col("recent_days").isin(7, 30))) \
        .select(
        "recent_days",
        expr("array(array(coalesce(driver1_emp_id, 0), driver1_name), array(coalesce(driver2_emp_id, 0), driver2_name)) as driver_arr"),
        "trans_finish_count",
        "trans_finish_distance",
        "trans_finish_dur_sec",
        "trans_finish_delay_count"
    ) \
        .selectExpr(
        "recent_days",
        "trans_finish_count",
        "trans_finish_distance / 2 as trans_finish_distance",
        "trans_finish_dur_sec / 2 as trans_finish_dur_sec",
        "trans_finish_delay_count",
        "explode(driver_arr) as driver_info"
    ) \
        .select(
        "recent_days",
        col("driver_info")[0].cast(LongType()).alias("driver_id"),
        col("driver_info")[1].alias("driver_name"),
        "trans_finish_count",
        "trans_finish_distance",
        "trans_finish_dur_sec",
        "trans_finish_delay_count"
    )

    # 5. 合并司机数据并过滤无效ID
    all_driver = single_driver.unionByName(double_driver) \
        .filter(col("driver_id") != 0)

    # 6. 聚合最终数据
    final_data = all_driver.groupBy("recent_days", "driver_id", "driver_name") \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        when(
            sum("trans_finish_count") == 0, lit(0)
        ).otherwise(
            round(sum("trans_finish_distance") / sum("trans_finish_count"), 2)
        ).alias("avg_trans_finish_distance"),
        when(
            sum("trans_finish_count") == 0, lit(0)
        ).otherwise(
            round(sum("trans_finish_dur_sec") / sum("trans_finish_count")).cast(LongType())
        ).alias("avg_trans_finish_dur_sec"),
        sum("trans_finish_delay_count").alias("trans_finish_late_count")
    ) \
        .withColumn("ds", lit(ds)) \
        .withColumnRenamed("driver_id", "driver_emp_id")

    # 7. 写入表
    insert_to_hive(final_data, "ads_driver_stats", ds)


def etl_ads_express_city_stats(spark, ds):
    """各城市快递统计表（DSL实现）"""
    # 1. 定义表结构
    ads_express_city_stats_schema = StructType([
        StructField("ds", StringType(), comment="统计日期"),
        StructField("recent_days", IntegerType(), comment="最近天数（1/7/30天）"),
        StructField("city_id", LongType(), comment="城市ID"),
        StructField("city_name", StringType(), comment="城市名称"),
        StructField("receive_order_count", LongType(), comment="揽收次数"),
        StructField("receive_order_amount", DecimalType(16, 2), comment="揽收金额"),
        StructField("deliver_suc_count", LongType(), comment="派送成功次数"),
        StructField("sort_count", LongType(), comment="分拣次数")
    ])

    # 2. 删除旧表并创建新表
    spark.sql("drop table if exists ads_express_city_stats")
    spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=ads_express_city_stats_schema) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("sep", "\t") \
        .option("path", "/warehouse/tms/ads/ads_express_city_stats") \
        .option("comment", "各城市快递统计") \
        .saveAsTable("tms.ads_express_city_stats")

    # 3. 最近1天数据
    # 3.1 派送数据
    deliver_1d = spark.table("dws_trans_org_deliver_suc_1d") \
        .filter(col("ds") == ds) \
        .groupBy("city_id", "city_name") \
        .agg(sum("order_count").alias("deliver_suc_count")) \
        .withColumn("ds", lit(ds)) \
        .withColumn("recent_days", lit(1))

    # 3.2 分拣数据
    sort_1d = spark.table("dws_trans_org_sort_1d") \
        .filter(col("ds") == ds) \
        .groupBy("city_id", "city_name") \
        .agg(sum("sort_count").alias("sort_count")) \
        .withColumn("ds", lit(ds)) \
        .withColumn("recent_days", lit(1))

    # 3.3 揽收数据
    receive_1d = spark.table("dws_trans_org_receive_1d") \
        .filter(col("ds") == ds) \
        .groupBy("city_id", "city_name") \
        .agg(
        sum("order_count").alias("receive_order_count"),
        sum("order_amount").alias("receive_order_amount")
    ) \
        .withColumn("ds", lit(ds)) \
        .withColumn("recent_days", lit(1))

    # 3.4 关联1天数据
    day1_data = deliver_1d.join(sort_1d, on=["ds", "recent_days", "city_id", "city_name"], how="full_outer") \
        .join(receive_1d, on=["ds", "recent_days", "city_id", "city_name"], how="full_outer") \
        .select(
        coalesce(deliver_1d["ds"], sort_1d["ds"], receive_1d["ds"]).alias("ds"),
        coalesce(deliver_1d["recent_days"], sort_1d["recent_days"], receive_1d["recent_days"]).alias("recent_days"),
        coalesce(deliver_1d["city_id"], sort_1d["city_id"], receive_1d["city_id"]).alias("city_id"),
        coalesce(deliver_1d["city_name"], sort_1d["city_name"], receive_1d["city_name"]).alias("city_name"),
        col("receive_order_count"),
        col("receive_order_amount"),
        col("deliver_suc_count"),
        col("sort_count")
    )

    # 4. 最近7/30天数据
    deliver_nd = spark.table("dws_trans_org_deliver_suc_nd") \
        .filter(col("ds") == ds) \
        .groupBy("recent_days", "city_id", "city_name") \
        .agg(sum("order_count").alias("deliver_suc_count")) \
        .withColumn("ds", lit(ds))

    sort_nd = spark.table("dws_trans_org_sort_nd") \
        .filter(col("ds") == ds) \
        .groupBy("recent_days", "city_id", "city_name") \
        .agg(sum("sort_count").alias("sort_count")) \
        .withColumn("ds", lit(ds))

    receive_nd = spark.table("dws_trans_org_receive_nd") \
        .filter(col("ds") == ds) \
        .groupBy("recent_days", "city_id", "city_name") \
        .agg(
        sum("order_count").alias("receive_order_count"),
        sum("order_amount").alias("receive_order_amount")
    ) \
        .withColumn("ds", lit(ds))

    # 4.1 关联多天数据
    nd_data = deliver_nd.join(sort_nd, on=["ds", "recent_days", "city_id", "city_name"], how="full_outer") \
        .join(receive_nd, on=["ds", "recent_days", "city_id", "city_name"], how="full_outer") \
        .select(
        coalesce(deliver_nd["ds"], sort_nd["ds"], receive_nd["ds"]).alias("ds"),
        coalesce(deliver_nd["recent_days"], sort_nd["recent_days"], receive_nd["recent_days"]).alias("recent_days"),
        coalesce(deliver_nd["city_id"], sort_nd["city_id"], receive_nd["city_id"]).alias("city_id"),
        coalesce(deliver_nd["city_name"], sort_nd["city_name"], receive_nd["city_name"]).alias("city_name"),
        col("receive_order_count"),
        col("receive_order_amount"),
        col("deliver_suc_count"),
        col("sort_count")
    )

    # 5. 合并数据并写入
    final_data = day1_data.unionByName(nd_data)
    insert_to_hive(final_data, "ads_express_city_stats", ds)


# 其他ETL函数（etl_ads_express_org_stats、etl_ads_express_province_stats等）逻辑类似，
# 均遵循“定义schema→创建表→构建子数据→关联聚合→写入”流程，此处省略（完整代码需按相同逻辑实现）


def main(partition_date):
    spark = get_spark_session()
    # 执行各ADS表ETL（此处仅示例3个核心表，完整代码需补充其他表）
    etl_ads_city_stats(spark, partition_date)
    etl_ads_driver_stats(spark, partition_date)
    etl_ads_express_city_stats(spark, partition_date)
    # ... 其他ETL函数调用
    print(f"[SUCCESS] All TMS ADS tables ETL completed for ds={partition_date}")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit tms_ads_dsl.py <partition_date>")
        sys.exit(1)
    ds = sys.argv[1]
main(ds)