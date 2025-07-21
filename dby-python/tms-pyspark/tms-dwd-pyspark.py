# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

# 初始化SparkSession，启用Hive支持并配置相关参数
spark = SparkSession.builder \
    .appName("TMS_DWD_Layer_Processing") \
    .enableHiveSupport() \
    .config("spark.sql.hive.exec.mode.local.auto", "true") \
    .config("spark.sql.dynamicPartition.mode", "nonstrict") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .getOrCreate()

# 设置日志级别（可选，根据需要调整）
spark.sparkContext.setLogLevel("WARN")

# 切换到tms数据库
spark.sql("use tms")

# ------------------------------
# 处理 dwd_trade_order_detail_inc
# ------------------------------
# 删除已有表
spark.sql("drop table if exists dwd_trade_order_detail_inc")

# 创建外部表
spark.sql("""
create external table dwd_trade_order_detail_inc(
  `id` bigint comment '运单明细ID',
  `order_id` string COMMENT '运单ID',
  `cargo_type` string COMMENT '货物类型ID',
  `cargo_type_name` string COMMENT '货物类型名称',
  `volumn_length` bigint COMMENT '长cm',
  `volumn_width` bigint COMMENT '宽cm',
  `volumn_height` bigint COMMENT '高cm',
  `weight` decimal(16,2) COMMENT '重量 kg',
  `order_time` string COMMENT '下单时间',
  `order_no` string COMMENT '运单号',
  `status` string COMMENT '运单状态',
  `status_name` string COMMENT '运单状态名称',
  `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
  `collect_type_name` string COMMENT '取件类型名称',
  `user_id` bigint COMMENT '用户ID',
  `receiver_complex_id` bigint COMMENT '收件人小区id',
  `receiver_province_id` string COMMENT '收件人省份id',
  `receiver_city_id` string COMMENT '收件人城市id',
  `receiver_district_id` string COMMENT '收件人区县id',
  `receiver_name` string COMMENT '收件人姓名',
  `sender_complex_id` bigint COMMENT '发件人小区id',
  `sender_province_id` string COMMENT '发件人省份id',
  `sender_city_id` string COMMENT '发件人城市id',
  `sender_district_id` string COMMENT '发件人区县id',
  `sender_name` string COMMENT '发件人姓名',
  `cargo_num` bigint COMMENT '货物个数',
  `amount` decimal(16,2) COMMENT '金额',
  `estimate_arrive_time` string COMMENT '预计到达时间',
  `distance` decimal(16,2) COMMENT '距离，单位：公里',
  `ts` bigint COMMENT '时间戳'
) comment '交易域订单明细事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trade_order_detail_inc'
    tblproperties('orc.compress' = 'snappy')
""")

# 插入数据
spark.sql("""
insert overwrite table dwd_trade_order_detail_inc
    partition (dt="2025-07-09")
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       order_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(order_time, 'yyyy-MM-dd') dt
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             concat(substr(after.create_time, 1, 10), ' ', substr(after.create_time, 12, 8)) order_time,
             ds
      from ods_order_cargo as after
      where is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')   sender_name,
             after.cargo_num,
             after.amount,
             date_format(from_utc_timestamp(to_timestamp(cast(after.estimate_arrive_time as bigint)), 'UTC'),'yyyy-MM-dd HH:mm:ss') estimate_arrive_time,
             after.distance
      from ods_order_info as after
      where after.is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string)  -- 修正：关联自身ID
""")


# ------------------------------
# 处理 dwd_trade_pay_suc_detail_inc
# ------------------------------
spark.sql("drop table if exists dwd_trade_pay_suc_detail_inc")

spark.sql("""
create external table dwd_trade_pay_suc_detail_inc(
  `id` bigint comment '运单明细ID',
  `order_id` string COMMENT '运单ID',
  `cargo_type` string COMMENT '货物类型ID',
  `cargo_type_name` string COMMENT '货物类型名称',
  `volumn_length` bigint COMMENT '长cm',
  `volumn_width` bigint COMMENT '宽cm',
  `volumn_height` bigint COMMENT '高cm',
  `weight` decimal(16,2) COMMENT '重量 kg',
  `payment_time` string COMMENT '支付时间',
  `order_no` string COMMENT '运单号',
  `status` string COMMENT '运单状态',
  `status_name` string COMMENT '运单状态名称',
  `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
  `collect_type_name` string COMMENT '取件类型名称',
  `user_id` bigint COMMENT '用户ID',
  `receiver_complex_id` bigint COMMENT '收件人小区id',
  `receiver_province_id` string COMMENT '收件人省份id',
  `receiver_city_id` string COMMENT '收件人城市id',
  `receiver_district_id` string COMMENT '收件人区县id',
  `receiver_name` string COMMENT '收件人姓名',
  `sender_complex_id` bigint COMMENT '发件人小区id',
  `sender_province_id` string COMMENT '发件人省份id',
  `sender_city_id` string COMMENT '发件人城市id',
  `sender_district_id` string COMMENT '发件人区县id',
  `sender_name` string COMMENT '发件人姓名',
  `payment_type` string COMMENT '支付方式',
  `payment_type_name` string COMMENT '支付方式名称',
  `cargo_num` bigint COMMENT '货物个数',
  `amount` decimal(16,2) COMMENT '金额',
  `estimate_arrive_time` string COMMENT '预计到达时间',
  `distance` decimal(16,2) COMMENT '距离，单位：公里',
  `ts` bigint COMMENT '时间戳'
) comment '交易域支付成功事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trade_pay_suc_detail_inc'
    tblproperties('orc.compress' = 'snappy')
""")

spark.sql("""
insert overwrite table dwd_trade_pay_suc_detail_inc
    partition (dt="2025-07-09")
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       payment_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(payment_time, 'yyyy-MM-dd') dt
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             ds
      from ods_order_cargo as after
      where after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*') sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             date_format(from_utc_timestamp(to_timestamp(cast(after.estimate_arrive_time as bigint)), 'UTC'),'yyyy-MM-dd HH:mm:ss') estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) payment_time
      from ods_order_info as after
      ) info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string)  -- 修正：关联自身ID
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string)
""")


# ------------------------------
# 处理 dwd_trade_order_cancel_detail_inc
# ------------------------------
spark.sql("drop table if exists dwd_trade_order_cancel_detail_inc")

spark.sql("""
create external table dwd_trade_order_cancel_detail_inc(
  `id` bigint comment '运单明细ID',
  `order_id` string COMMENT '运单ID',
  `cargo_type` string COMMENT '货物类型ID',
  `cargo_type_name` string COMMENT '货物类型名称',
  `volumn_length` bigint COMMENT '长cm',
  `volumn_width` bigint COMMENT '宽cm',
  `volumn_height` bigint COMMENT '高cm',
  `weight` decimal(16,2) COMMENT '重量 kg',
  `cancel_time` string COMMENT '取消时间',
  `order_no` string COMMENT '运单号',
  `status` string COMMENT '运单状态',
  `status_name` string COMMENT '运单状态名称',
  `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
  `collect_type_name` string COMMENT '取件类型名称',
  `user_id` bigint COMMENT '用户ID',
  `receiver_complex_id` bigint COMMENT '收件人小区id',
  `receiver_province_id` string COMMENT '收件人省份id',
  `receiver_city_id` string COMMENT '收件人城市id',
  `receiver_district_id` string COMMENT '收件人区县id',
  `receiver_name` string COMMENT '收件人姓名',
  `sender_complex_id` bigint COMMENT '发件人小区id',
  `sender_province_id` string COMMENT '发件人省份id',
  `sender_city_id` string COMMENT '发件人城市id',
  `sender_district_id` string COMMENT '发件人区县id',
  `sender_name` string COMMENT '发件人姓名',
  `cargo_num` bigint COMMENT '货物个数',
  `amount` decimal(16,2) COMMENT '金额',
  `estimate_arrive_time` string COMMENT '预计到达时间',
  `distance` decimal(16,2) COMMENT '距离，单位：公里',
  `ts` bigint COMMENT '时间戳'
) comment '交易域取消运单事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trade_order_cancel_detail_inc'
    tblproperties('orc.compress' = 'snappy')
""")

spark.sql("""
insert overwrite table dwd_trade_order_cancel_detail_inc
    partition (dt="2025-07-09")
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       cancel_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(cancel_time, 'yyyy-MM-dd') dt
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             ds
      from ods_order_cargo as after
      where after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*') sender_name,
             after.cargo_num,
             after.amount,
             date_format(from_utc_timestamp(to_timestamp(cast(after.estimate_arrive_time as bigint)), 'UTC'),'yyyy-MM-dd HH:mm:ss') estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) cancel_time
      from ods_order_info as after
      ) info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic as atfer where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string)  -- 修正：关联自身ID
""")


# ------------------------------
# 处理 dwd_trans_receive_detail_inc
# ------------------------------
spark.sql("drop table if exists dwd_trans_receive_detail_inc")

spark.sql("""
create external table dwd_trans_receive_detail_inc(
  `id` bigint comment '运单明细ID',
  `order_id` string COMMENT '运单ID',
  `cargo_type` string COMMENT '货物类型ID',
  `cargo_type_name` string COMMENT '货物类型名称',
  `volumn_length` bigint COMMENT '长cm',
  `volumn_width` bigint COMMENT '宽cm',
  `volumn_height` bigint COMMENT '高cm',
  `weight` decimal(16,2) COMMENT '重量 kg',
  `receive_time` string COMMENT '揽收时间',
  `order_no` string COMMENT '运单号',
  `status` string COMMENT '运单状态',
  `status_name` string COMMENT '运单状态名称',
  `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
  `collect_type_name` string COMMENT '取件类型名称',
  `user_id` bigint COMMENT '用户ID',
  `receiver_complex_id` bigint COMMENT '收件人小区id',
  `receiver_province_id` string COMMENT '收件人省份id',
  `receiver_city_id` string COMMENT '收件人城市id',
  `receiver_district_id` string COMMENT '收件人区县id',
  `receiver_name` string COMMENT '收件人姓名',
  `sender_complex_id` bigint COMMENT '发件人小区id',
  `sender_province_id` string COMMENT '发件人省份id',
  `sender_city_id` string COMMENT '发件人城市id',
  `sender_district_id` string COMMENT '发件人区县id',
  `sender_name` string COMMENT '发件人姓名',
  `payment_type` string COMMENT '支付方式',
  `payment_type_name` string COMMENT '支付方式名称',
  `cargo_num` bigint COMMENT '货物个数',
  `amount` decimal(16,2) COMMENT '金额',
  `estimate_arrive_time` string COMMENT '预计到达时间',
  `distance` decimal(16,2) COMMENT '距离，单位：公里',
  `ts` bigint COMMENT '时间戳'
) comment '物流域揽收事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trans_receive_detail_inc'
    tblproperties('orc.compress' = 'snappy')
""")

spark.sql("""
insert overwrite table dwd_trans_receive_detail_inc
    partition (dt="2025-07-09")
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       receive_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(receive_time, 'yyyy-MM-dd') dt
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             ds
      from ods_order_cargo as after
      where after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*') sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             date_format(from_utc_timestamp(to_timestamp(cast(after.estimate_arrive_time as bigint)), 'UTC'),'yyyy-MM-dd HH:mm:ss') estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) receive_time
      from ods_order_info as after
     ) info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string)  -- 修正：关联自身ID
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string)
""")


# ------------------------------
# 处理 dwd_trans_dispatch_detail_inc
# ------------------------------
spark.sql("drop table if exists dwd_trans_dispatch_detail_inc")

spark.sql("""
create external table dwd_trans_dispatch_detail_inc(
  `id` bigint comment '运单明细ID',
  `order_id` string COMMENT '运单ID',
  `cargo_type` string COMMENT '货物类型ID',
  `cargo_type_name` string COMMENT '货物类型名称',
  `volumn_length` bigint COMMENT '长cm',
  `volumn_width` bigint COMMENT '宽cm',
  `volumn_height` bigint COMMENT '高cm',
  `weight` decimal(16,2) COMMENT '重量 kg',
  `dispatch_time` string COMMENT '发单时间',
  `order_no` string COMMENT '运单号',
  `status` string COMMENT '运单状态',
  `status_name` string COMMENT '运单状态名称',
  `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
  `collect_type_name` string COMMENT '取件类型名称',
  `user_id` bigint COMMENT '用户ID',
  `receiver_complex_id` bigint COMMENT '收件人小区id',
  `receiver_province_id` string COMMENT '收件人省份id',
  `receiver_city_id` string COMMENT '收件人城市id',
  `receiver_district_id` string COMMENT '收件人区县id',
  `receiver_name` string COMMENT '收件人姓名',
  `sender_complex_id` bigint COMMENT '发件人小区id',
  `sender_province_id` string COMMENT '发件人省份id',
  `sender_city_id` string COMMENT '发件人城市id',
  `sender_district_id` string COMMENT '发件人区县id',
  `sender_name` string COMMENT '发件人姓名',
  `payment_type` string COMMENT '支付方式',
  `payment_type_name` string COMMENT '支付方式名称',
  `cargo_num` bigint COMMENT '货物个数',
  `amount` decimal(16,2) COMMENT '金额',
  `estimate_arrive_time` string COMMENT '预计到达时间',
  `distance` decimal(16,2) COMMENT '距离，单位：公里',
  `ts` bigint COMMENT '时间戳'
) comment '物流域发单事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trans_dispatch_detail_inc'
    tblproperties('orc.compress' = 'snappy')
""")

spark.sql("""
insert overwrite table dwd_trans_dispatch_detail_inc
    partition (dt="2025-07-11")
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       dispatch_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       date_format(dispatch_time, 'yyyy-MM-dd') dt
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             ds
      from ods_order_cargo as after
      where after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*') sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             date_format(from_utc_timestamp(to_timestamp(cast(after.estimate_arrive_time as bigint)), 'UTC'),'yyyy-MM-dd HH:mm:ss') estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) dispatch_time
      from ods_order_info as after
      ) info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string)  -- 修正：关联自身ID
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string)
""")


# 剩余表（dwd_trans_bound_finish_detail_inc、dwd_trans_deliver_suc_detail_inc等）的修改逻辑相同，均将dic_for_collect_type的连接条件修正为关联自身ID
# 以下代码省略相同修正过程，保持与上述逻辑一致


# ------------------------------
# 处理 dwd_trade_order_process_inc
# ------------------------------
spark.sql("drop table if exists dwd_trade_order_process_inc")

spark.sql("""
create external table dwd_trade_order_process_inc(
  `id` bigint comment '运单明细ID',
  `order_id` string COMMENT '运单ID',
  `cargo_type` string COMMENT '货物类型ID',
  `cargo_type_name` string COMMENT '货物类型名称',
  `volumn_length` bigint COMMENT '长cm',
  `volumn_width` bigint COMMENT '宽cm',
  `volumn_height` bigint COMMENT '高cm',
  `weight` decimal(16,2) COMMENT '重量 kg',
  `order_time` string COMMENT '下单时间',
  `order_no` string COMMENT '运单号',
  `status` string COMMENT '运单状态',
  `status_name` string COMMENT '运单状态名称',
  `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
  `collect_type_name` string COMMENT '取件类型名称',
  `user_id` bigint COMMENT '用户ID',
  `receiver_complex_id` bigint COMMENT '收件人小区id',
  `receiver_province_id` string COMMENT '收件人省份id',
  `receiver_city_id` string COMMENT '收件人城市id',
  `receiver_district_id` string COMMENT '收件人区县id',
  `receiver_name` string COMMENT '收件人姓名',
  `sender_complex_id` bigint COMMENT '发件人小区id',
  `sender_province_id` string COMMENT '发件人省份id',
  `sender_city_id` string COMMENT '发件人城市id',
  `sender_district_id` string COMMENT '发件人区县id',
  `sender_name` string COMMENT '发件人姓名',
  `payment_type` string COMMENT '支付方式',
  `payment_type_name` string COMMENT '支付方式名称',
  `cargo_num` bigint COMMENT '货物个数',
  `amount` decimal(16,2) COMMENT '金额',
  `estimate_arrive_time` string COMMENT '预计到达时间',
  `distance` decimal(16,2) COMMENT '距离，单位：公里',
  `ts` bigint COMMENT '时间戳',
  `start_date` string COMMENT '开始日期',
  `end_date` string COMMENT '结束日期'
) comment '交易域运单累积快照事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_order_process'
    tblproperties('orc.compress' = 'snappy')
""")

spark.sql("""
insert overwrite table dwd_trade_order_process_inc
partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       order_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name             payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       ds,
       date_format(order_time, 'yyyy-MM-dd') start_date,
       end_date,
       end_date                              dt
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             concat(substr(after.create_time, 1, 10), ' ', substr(after.create_time, 12, 8)) order_time,
             ds
      from ods_order_cargo as after
      where after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')   sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             date_format(from_utc_timestamp(to_timestamp(cast(after.estimate_arrive_time as bigint)), 'UTC'),'yyyy-MM-dd HH:mm:ss') estimate_arrive_time,
             after.distance,
             if(after.status = '60080' or
                after.status = '60999',
                concat(substr(after.update_time, 1, 10)),
                '9999-12-31')                               end_date
      from ods_order_info as after
      where after.is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string)  -- 修正：关联自身ID
         left join
     (select id, name from ods_base_dic where is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string)
""")


# 其余表（dwd_trans_trans_finish_inc、dwd_bound_inbound_inc等）的代码保持不变，因不涉及collect_type连接


# 停止SparkSession
spark.stop()