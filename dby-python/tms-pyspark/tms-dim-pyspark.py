# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys

def get_spark_session():
    """获取配置好的SparkSession对象"""
    spark = SparkSession.builder \
        .appName("TmsDimETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE tms")  # 切换到tms数据库
    return spark

def insert_to_hive(df, table_name, partition_date):
    """将DataFrame写入Hive分区表，自动添加dt分区字段"""
    if df.rdd.isEmpty():
        print(f"[WARN] No data found for {table_name} on {partition_date}, skipping write.")
        return
    # 所有表均使用dt作为分区字段
    df_with_partition = df.withColumn("dt", lit(partition_date))
    df_with_partition.write.mode("overwrite").insertInto(f"tms.{table_name}")

def etl_dim_complex_full(spark, ds):
    """小区维度表ETL处理"""
    sql = f"""
    SELECT
      complex_info.id,
      complex_name,
      courier_emp_ids,
      province_id,
      dic_for_prov.name AS province_name,
      city_id,
      dic_for_city.name AS city_name,
      district_id,
      district_name
    FROM (
      SELECT id, complex_name, province_id, city_id, district_id, district_name
      FROM ods_base_complex
      WHERE ds = '{ds}' AND is_deleted = '0'
    ) complex_info
    JOIN (
      SELECT id, name
      FROM ods_base_region_info
      WHERE ds = '20200623' AND is_deleted = '0'
    ) dic_for_prov ON complex_info.province_id = dic_for_prov.id
    JOIN (
      SELECT id, name
      FROM ods_base_region_info
      WHERE ds = '20200623' AND is_deleted = '0'
    ) dic_for_city ON complex_info.city_id = dic_for_city.id
    LEFT JOIN (
      SELECT 
        collect_set(cast(courier_emp_id as string)) AS courier_emp_ids, 
        complex_id
      FROM ods_express_courier_complex
      WHERE ds = '{ds}' AND is_deleted = '0'
      GROUP BY complex_id
    ) complex_courier ON complex_info.id = complex_courier.complex_id
    """
    df = spark.sql(sql)
    insert_to_hive(df, "dim_complex_full", ds)

def etl_dim_organ_full(spark, ds):
    """机构维度表ETL处理"""
    sql = f"""
    SELECT
      organ_info.id,
      organ_info.org_name,
      org_level,
      region_id,
      region_info.name AS region_name,
      region_info.dict_code AS region_code,
      org_parent_id,
      org_for_parent.org_name AS org_parent_name
    FROM (
      SELECT id, org_name, org_level, region_id, org_parent_id
      FROM ods_base_organ
      WHERE ds = '{ds}' AND is_deleted = '0'
    ) organ_info
    LEFT JOIN (
      SELECT id, name, dict_code
      FROM ods_base_region_info
      WHERE ds = '20200623' AND is_deleted = '0'
    ) region_info ON organ_info.region_id = region_info.id
    LEFT JOIN (
      SELECT id, org_name
      FROM ods_base_organ
      WHERE ds = '{ds}' AND is_deleted = '0'
    ) org_for_parent ON organ_info.org_parent_id = org_for_parent.id
    """
    df = spark.sql(sql)
    insert_to_hive(df, "dim_organ_full", ds)

def etl_dim_region_full(spark, ds):
    """地区维度表ETL处理"""
    sql = f"""
    SELECT
      id,
      parent_id,
      name,
      dict_code,
      short_name
    FROM ods_base_region_info
    WHERE ds = '20200623' AND is_deleted = '0'
    """
    df = spark.sql(sql)
    insert_to_hive(df, "dim_region_full", ds)

def etl_dim_express_courier_full(spark, ds):
    """快递员维度表ETL处理"""
    sql = f"""
    SELECT
      express_cor_info.id,
      emp_id,
      org_id,
      org_name,
      working_phone,
      express_type,
      dic_info.name AS express_type_name
    FROM (
      SELECT 
        id, 
        emp_id, 
        org_id, 
        md5(working_phone) AS working_phone, 
        express_type
      FROM ods_express_courier
      WHERE ds = '20200106' AND is_deleted = '0'
    ) express_cor_info
    JOIN (
      SELECT id, org_name
      FROM ods_base_organ
      WHERE ds = '{ds}' AND is_deleted = '0'
    ) organ_info ON express_cor_info.org_id = organ_info.id
    JOIN (
      SELECT id, name
      FROM ods_base_dic
      WHERE ds = '20220708' AND is_deleted = '0'
    ) dic_info ON express_cor_info.express_type = dic_info.id
    """
    df = spark.sql(sql)
    insert_to_hive(df, "dim_express_courier_full", ds)

def etl_dim_shift_full(spark, ds):
    """班次维度表ETL处理"""
    sql = f"""
    SELECT
      shift_info.id,
      line_id,
      line_info.name AS line_name,
      line_no,
      line_level,
      org_id,
      transport_line_type_id,
      dic_info.name AS transport_line_type_name,
      start_org_id,
      start_org_name,
      end_org_id,
      end_org_name,
      pair_line_id,
      distance,
      cost,
      estimated_time,
      start_time,
      driver1_emp_id,
      driver2_emp_id,
      truck_id,
      pair_shift_id
    FROM (
      SELECT 
        id, 
        line_id, 
        start_time, 
        driver1_emp_id, 
        driver2_emp_id, 
        truck_id, 
        pair_shift_id
      FROM ods_line_base_shift
      WHERE ds = '{ds}' AND is_deleted = '0'
    ) shift_info
    JOIN (
      SELECT 
        id,
        name,
        line_no,
        line_level,
        org_id,
        transport_line_type_id,
        start_org_id,
        start_org_name,
        end_org_id,
        end_org_name,
        pair_line_id,
        distance,
        cost,
        estimated_time
      FROM ods_line_base_info
      WHERE ds = '20230106' AND is_deleted = '0'
    ) line_info ON shift_info.line_id = line_info.id
    JOIN (
      SELECT id, name
      FROM ods_base_dic
      WHERE ds = '20220708' AND is_deleted = '0'
    ) dic_info ON line_info.transport_line_type_id = dic_info.id
    """
    df = spark.sql(sql)
    insert_to_hive(df, "dim_shift_full", ds)

def etl_dim_truck_driver_full(spark, ds):
    """司机维度表ETL处理"""
    sql = f"""
    SELECT
      driver_info.id,
      emp_id,
      org_id,
      organ_info.org_name,
      team_id,
      team_info.name AS team_name,
      license_type,
      init_license_date,
      expire_date,
      license_no,
      is_enabled
    FROM (
      SELECT 
        id, 
        emp_id, 
        org_id, 
        team_id, 
        license_type, 
        init_license_date, 
        expire_date, 
        license_no, 
        is_enabled
      FROM ods_truck_driver
      WHERE is_deleted = '0'
    ) driver_info
    JOIN (
      SELECT id, org_name
      FROM ods_base_organ
    ) organ_info ON driver_info.org_id = organ_info.id
    JOIN (
      SELECT id, name
      FROM ods_truck_team
    ) team_info ON driver_info.team_id = team_info.id
    """
    df = spark.sql(sql)
    insert_to_hive(df, "dim_truck_driver_full", ds)

def etl_dim_truck_full(spark, ds):
    """卡车维度表ETL处理"""
    sql = f"""
    SELECT
      truck_info.id,
      team_id,
      team_info.name AS team_name,
      team_no,
      org_id,
      org_name,
      manager_emp_id,
      truck_no,
      truck_model_id,
      model_info.model_name AS truck_model_name,
      model_info.model_type AS truck_model_type,
      dic_for_type.name AS truck_model_type_name,
      model_info.model_no AS truck_model_no,
      model_info.brand AS truck_brand,
      dic_for_brand.name AS truck_brand_name,
      model_info.truck_weight,
      model_info.load_weight,
      model_info.total_weight,
      model_info.eev,
      model_info.boxcar_len,
      model_info.boxcar_wd,
      model_info.boxcar_hg,
      model_info.max_speed,
      model_info.oil_vol,
      device_gps_id,
      engine_no,
      license_registration_date,
      license_last_check_date,
      license_expire_date,
      is_enabled
    FROM (
      SELECT 
        id, 
        team_id, 
        md5(truck_no) AS truck_no, 
        truck_model_id, 
        device_gps_id, 
        engine_no, 
        license_registration_date, 
        license_last_check_date, 
        license_expire_date, 
        is_enabled
      FROM ods_truck_info
      WHERE is_deleted = '0'
    ) truck_info
    JOIN (
      SELECT 
        id, 
        name, 
        team_no, 
        org_id, 
        manager_emp_id
      FROM ods_truck_team
      WHERE is_deleted = '0'
    ) team_info ON truck_info.team_id = team_info.id
    JOIN (
      SELECT 
        id,
        model_name,
        model_type,
        model_no,
        brand,
        truck_weight,
        load_weight,
        total_weight,
        eev,
        boxcar_len,
        boxcar_wd,
        boxcar_hg,
        max_speed,
        oil_vol
      FROM ods_truck_model
      WHERE is_deleted = '0'
    ) model_info ON truck_info.truck_model_id = model_info.id
    JOIN (
      SELECT id, org_name
      FROM ods_base_organ
      WHERE is_deleted = '0'
    ) organ_info ON team_info.org_id = organ_info.id
    JOIN (
      SELECT id, name
      FROM ods_base_dic
      WHERE is_deleted = '0'
    ) dic_for_type ON model_info.model_type = dic_for_type.id
    JOIN (
      SELECT id, name
      FROM ods_base_dic
      WHERE is_deleted = '0'
    ) dic_for_brand ON model_info.brand = dic_for_brand.id
    """
    df = spark.sql(sql)
    insert_to_hive(df, "dim_truck_full", ds)

def etl_dim_user_zip(spark, ds):
    """用户拉链表ETL处理"""
    sql = f"""
    SELECT
      after.id,
      after.login_name,
      after.nick_name,
      md5(after.passwd) AS passwd,
      md5(after.real_name) AS real_name,
      md5(when(
        after.phone_num rlike '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
        after.phone_num
      )) AS phone_num,
      md5(when(
        after.email rlike '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
        after.email
      )) AS email,
      after.user_level,
      date_add('1970-01-01', cast(after.birthday as int)) AS birthday,
      after.gender,
      date_format(from_utc_timestamp(cast(after.create_time as bigint), 'UTC'), 'yyyy-MM-dd') AS start_date,
      '9999-12-31' AS end_date
    FROM ods_user_info AS after
    WHERE after.is_deleted = '0'
    """
    df = spark.sql(sql)
    insert_to_hive(df, "dim_user_zip", ds)

def etl_dim_user_address_zip(spark, ds):
    """用户地址拉链表ETL处理"""
    sql = f"""
    SELECT
      after.id,
      after.user_id,
      md5(when(
        after.phone rlike '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
        after.phone
      )) AS phone,
      after.province_id,
      after.city_id,
      after.district_id,
      after.complex_id,
      after.address,
      after.is_default,
      concat(substr(after.create_time, 1, 10), ' ', substr(after.create_time, 12, 8)) AS start_date,
      '9999-12-31' AS end_date
    FROM ods_user_address AS after
    WHERE ds = '{ds}' AND after.is_deleted = '0'
    """
    df = spark.sql(sql)
    insert_to_hive(df, "dim_user_address_zip", ds)

def main(partition_date):
    """主函数：执行所有维度表的ETL流程"""
    spark = get_spark_session()

    # 执行各维度表ETL
    etl_dim_complex_full(spark, partition_date)
    etl_dim_organ_full(spark, partition_date)
    etl_dim_region_full(spark, partition_date)
    etl_dim_express_courier_full(spark, partition_date)
    etl_dim_shift_full(spark, partition_date)
    etl_dim_truck_driver_full(spark, partition_date)
    etl_dim_truck_full(spark, partition_date)
    etl_dim_user_zip(spark, partition_date)
    etl_dim_user_address_zip(spark, partition_date)

    print(f"[SUCCESS] All dimension tables ETL completed for dt={partition_date}")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit this_script.py <partition_date>")
        sys.exit(1)
    dt = sys.argv[1]
    main(dt)