set hive.exec.mode.local.auto=true;
create database if not exists gd_4and9;
use gd_4and9;

-- todo:一级品类表
drop table if exists ods_base_category1;
create table if not exists ods_base_category1
(
    `id`   string comment 'ID',
    `name` string comment '名称'
) comment '商品一级分类表'
    row format delimited fields terminated by "\t"
    stored as
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
load data inpath '/origin_data/gmall_s1/db/base_category1/2025-01-01' overwrite into table ods_base_category1;

select *  from ods_base_category1;

-- todo:二级品类表
drop table if exists ods_base_category2;
create table if not exists ods_base_category2
(
    `id`           string comment 'ID',
    `name`         string comment '名称',
    `category1_id` string comment '一级品类ID'
) comment '商品二级分类表'
    row format delimited fields terminated by "\t"
    stored as
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
load data inpath '/origin_data/gmall_s1/db/base_category2/2025-01-01' overwrite into table ods_base_category2;

select  *  from ods_base_category2;

-- todo:三级品类表
drop table if exists ods_base_category3;
create table if not exists ods_base_category3
(
    `id`           string comment 'ID',
    `name`         string comment '名称',
    `category2_id` string comment '二级品类ID'
) comment '商品三级分类表'
    row format delimited fields terminated by "\t"
    stored as
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
load data inpath '/origin_data/gmall_s1/db/base_category3/2025-01-01' overwrite into table ods_base_category3;

select *  from ods_base_category3;

-- todo:购物车表
drop table if exists ods_cart_info;
create table if not exists ods_cart_info
(
    `user_id`      string comment '用户ID',
    `sku_id`       string comment 'skuID',
    `create_time`  string comment '创建时间'
) comment '加购表'
    row format delimited fields terminated by "\t"
    stored as
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
load data inpath '/origin_data/gmall_s1/db/cart_info/2025-01-01' overwrite into table ods_cart_info;

select *  from ods_cart_info;

-- todo:收藏表
drop table if exists ods_favor_info;
create table if not exists ods_favor_info
(
    `sku_id`      string comment 'skuID',
    `is_cancel`   string comment '是否取消',
    `create_time` string comment '收藏时间'
) comment '商品收藏表'
    row format delimited fields terminated by "\t"
    stored as
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
load data inpath '/origin_data/gmall_s1/db/favor_info/2025-01-01' overwrite into table ods_favor_info;

select *  from ods_favor_info;

-- todo:订单明细表
drop table if exists ods_order_detail;
create table if not exists ods_order_detail
(
    `id`                    string comment '编号',
    `order_id`              string comment '订单号',
    `sku_id`                string comment '商品ID',
    `order_price`           decimal(16, 2) comment '商品价格',
    `sku_num`               bigint comment '商品数量',
    `split_final_amount`    decimal(16, 2) comment '分摊最终金额',
    `create_time`           string comment '创建时间'
) comment '订单详情表'
    row format delimited fields terminated by "\t"
    stored as
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
load data inpath '/origin_data/gmall_s1/db/order_detail/2025-01-01' overwrite into table ods_order_detail;

select *  from ods_order_detail;

-- todo:订单表
drop table if exists ods_order_info;
create table if not exists ods_order_info
(
    `id`                     string comment '订单号',
    `final_amount`           decimal(16, 2) comment '订单最终金额',
    `order_status`           string comment '订单状态',
    `user_id`                string comment '用户ID',
    `create_time`            string comment '创建时间'
) comment '订单表'
    row format delimited fields terminated by "\t"
    stored as
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
load data inpath '/origin_data/gmall_s1/db/order_info/2025-01-01' overwrite into table ods_order_info;

select *  from ods_order_info;

-- todo:支付表
drop table if exists ods_payment_info;
create table if not exists ods_payment_info
(
    `id`             string comment '编号',
    `order_id`       string comment '订单编号',
    `user_id`        string comment '用户编号',
    `callback_time`  string comment '回调时间',
    `create_time`    string comment '创建时间'
) comment '支付流水表'
    row format delimited fields terminated by "\t"
    stored as
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
load data inpath '/origin_data/gmall_s1/db/payment_info/2025-01-01' overwrite into table ods_payment_info;

select *  from ods_payment_info;

-- todo:商品表（SKU）
drop table if exists ods_sku_info;
create table if not exists ods_sku_info
(
    `id`           string comment 'skuID',
    `spu_id`       string comment 'spuID',
    `price`        decimal(16, 2) comment '价格',
    `sku_name`     string comment '商品名称',
    `category3_id` string comment '品类ID',
    `create_time`  string comment '创建时间'
) comment 'SKU商品表'
    row format delimited fields terminated by "\t"
    stored as
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
load data inpath '/origin_data/gmall_s1/db/sku_info/2025-01-01' overwrite into table ods_sku_info;

select *  from ods_sku_info;

-- todo:商品表（SPU）
drop table if exists ods_spu_info;
create table if not exists ods_spu_info
(
    `id`           string comment 'spuID',
    `spu_name`     string comment 'spu名称',
    `category3_id` string comment '品类ID'
) comment 'SPU商品表'
    row format delimited fields terminated by "\t"
    stored as
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
load data inpath '/origin_data/gmall_s1/db/spu_info/2025-01-01' overwrite into table ods_spu_info;

select *  from ods_spu_info;

-- todo:用户表
drop table if exists ods_user_info;
create table if not exists ods_user_info
(
    `id`           string comment '用户ID',
    `name`         string comment '用户姓名',
    `birthday`     string comment '生日',
    `create_time`  string comment '创建时间'
) comment '用户表'
    row format delimited fields terminated by "\t"
    stored as
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
load data inpath '/origin_data/gmall_s1/db/user_info/2025-01-01' overwrite into table ods_user_info;

select *  from ods_user_info;





---------------------------------------日志-------------------------------------------------
create external table if not exists ods_log(
    line string
)partitioned by (dt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
load data local  inpath '/opt/applog/log/app.2025-06-07.log' overwrite into table ods_log partition(dt='2025-01-01');

select * from ods_log;


---------------------------------------热搜-------------------------------------------------
drop table if exists hot_search_keywords;
CREATE TABLE IF NOT EXISTS hot_search_keywords (
                                                   `keyword` STRING COMMENT '搜索关键词',
                                                   `search_volume` BIGINT COMMENT '搜索量',
                                                   `visitor_count` BIGINT COMMENT '带来的访客数',
                                                   `category_id` STRING COMMENT '关联类目ID（可为空）'

)
    COMMENT '热搜词分析表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;

load data local inpath '/home/gd/hot.csv' overwrite into table hot_search_keywords;
select *
from hot_search_keywords;



