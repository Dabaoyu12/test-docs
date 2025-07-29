import os
import sys
# 强制指定Python路径（使用sys.executable获取当前运行的Python解释器）
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count, avg, when, to_timestamp
import datetime
import random

# 初始化SparkSession（单线程模式）
spark = SparkSession.builder \
    .appName("电商数仓-极简模式") \
    .master("local[1]")  # 强制单线程运行，避免资源竞争 \
.config("spark.sql.shuffle.partitions", "1")  # 仅1个分区 \
.config("spark.python.worker.timeout", "1200") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.maxResultSize", "512m") \
    .config("spark.python.worker.memory", "512m")  # 限制Python worker内存 \
.getOrCreate()


def generate_mock_data():
    """生成最小量数据（确保能运行）"""
    total_users = 50    # 仅50用户
    total_pages = 10    # 10个页面
    start_date = "2025-01-25"  # 缩短日期范围
    end_date = "2025-01-26"

    # 生成日期列表（仅2天）
    date_list = [
        datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=i)
        for i in range(2)
    ]
    date_str_list = [d.strftime("%Y-%m-%d") for d in date_list]

    # 页面类型与名称（极度简化）
    page_types = ["店铺页", "商品详情页"]
    page_type_map = {i: random.choice(page_types) for i in range(total_pages)}
    page_names = {
        "店铺页": ["首页", "活动页"],
        "商品详情页": ["商品1详情页", "商品2详情页"]
    }
    page_name_map = {
        i: random.choice(page_names[page_type_map[i]])
        for i in range(total_pages)
    }

    # 生成数据（每个用户仅1条记录）
    data = []
    for user_id in range(total_users):
        dt = random.choice(date_str_list)
        page_id = random.randint(0, total_pages - 1)
        data.append((
            user_id,
            page_id,
            page_type_map[page_id],
            page_name_map[page_id],
            -1,  # 简化：全部无来源页面
            f"{dt} 12:00:00",  # 固定时间，减少随机性
            0,  # 简化：全部不下单
            60,  # 固定停留时间
            "wireless" if random.random() > 0.5 else "pc",
            dt
        ))

    # 创建DataFrame
    df = spark.createDataFrame(data, [
        "user_id", "page_id", "page_type", "page_name", "refer_page_id",
        "visit_time", "is_order", "stay_time", "device_type", "dt"
    ]).withColumn("visit_time", to_timestamp(col("visit_time"), "yyyy-MM-dd HH:mm:ss"))

    return df


def calculate_wireless_indicators(clean_df, time_range="7days"):
    """简化的无线端指标计算"""
    end_date = "2025-01-26"
    start_date = "2025-01-25"  # 仅2天数据

    wireless_df = clean_df \
        .filter(col("device_type") == "wireless") \
        .filter(col("dt").between(start_date, end_date)) \
        .cache()

    # 仅保留核心指标
    entrance_df = wireless_df \
        .groupBy("page_id", "page_name") \
        .agg(
        countDistinct("user_id").alias("访客数")
    ) \
        .orderBy(col("访客数").desc())

    wireless_df.unpersist()
    return {"entrance_indicators": entrance_df}


def calculate_pc_indicators(clean_df):
    """简化的PC端指标计算"""
    pc_df = clean_df \
        .filter(col("device_type") == "pc") \
        .cache()

    # 仅保留核心指标
    pc_page_rank = pc_df \
        .groupBy("page_id", "page_name") \
        .agg(
        count("page_id").alias("浏览量")
    ) \
        .orderBy(col("浏览量").desc())

    pc_df.unpersist()
    return {"page_rank": pc_page_rank}


if __name__ == "__main__":
    try:
        print("开始生成极小量模拟数据...")
        mock_data = generate_mock_data()

        # 先显示部分数据确认格式正确
        print("\n数据样例:")
        mock_data.show(3)

        # 计算数据量（使用简化方式）
        print(f"\n生成模拟数据量: {mock_data.count()} 条")

        # 简化计算逻辑
        print("\n计算无线端指标...")
        wireless_results = calculate_wireless_indicators(mock_data)
        print("无线端页面访客数TOP3:")
        wireless_results["entrance_indicators"].show(3)

        print("\n计算PC端指标...")
        pc_results = calculate_pc_indicators(mock_data)
        print("PC端页面浏览量TOP3:")
        pc_results["page_rank"].show(3)

    except Exception as e:
        print(f"\n错误: {str(e)}")
    finally:
        spark.stop()
        print("\nSparkSession已关闭")
