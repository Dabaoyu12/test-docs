set hive.exec.mode.local.auto=true;
create database if not exists gd_4and9;
use gd_4and9;

set hive.exec.dynamic.partition.mode=nonstrict;

----------------TODO:爆炸函数------------------
add jar /home/jar/com.bw.Customize.jsonArrayUdtf.jar;
create temporary function json_array_object as "com.bw.Customize.jsonArrayUdtf";
----------------------------------

DROP TABLE IF EXISTS dwd_start_log;
CREATE EXTERNAL TABLE dwd_start_log(
                                       `area_code` STRING COMMENT '地区编码',
                                       `brand` STRING COMMENT '手机品牌',
                                       `channel` STRING COMMENT '渠道',
                                       `is_new` STRING COMMENT '是否首次启动',
                                       `model` STRING COMMENT '手机型号',
                                       `mid_id` STRING COMMENT '设备id',
                                       `os` STRING COMMENT '操作系统',
                                       `user_id` STRING COMMENT '会员id',
                                       `version_code` STRING COMMENT 'app版本号',
                                       `entry` STRING COMMENT 'icon手机图标 notice 通知 install 安装后启动',
                                       `loading_time` BIGINT COMMENT '启动加载时间',
                                       `open_ad_id` STRING COMMENT '广告页ID ',
                                       `open_ad_ms` BIGINT COMMENT '广告总共播放时间',
                                       `open_ad_skip_ms` BIGINT COMMENT '用户跳过广告时点',
                                       `ts` BIGINT COMMENT '时间'
) COMMENT '启动日志表'
    PARTITIONED BY (`dt` STRING) -- 按照时间创建分区
;-- 采用LZO压缩


insert overwrite table dwd_start_log
select
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.is_new'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.start.entry'),
    get_json_object(line,'$.start.loading_time'),
    get_json_object(line,'$.start.open_ad_id'),
    get_json_object(line,'$.start.open_ad_ms'),
    get_json_object(line,'$.start.open_ad_skip_ms'),
    get_json_object(line,'$.ts'),
    from_unixtime(cast(get_json_object(line,'$.ts')/1000 as bigint),'yyyy-MM-dd')
from ods_log
where  get_json_object(line,'$.start') is not null;
select *
from dwd_start_log;



---TODO:------------dwd_page_log-------------------
drop table dwd_page_log;
create table dwd_page_log(
                             ar string comment '地区',
                             ba string comment '品牌',
                             ch string comment '渠道',
                             is_new string comment '是否新用户',
                             md string comment '手机型号',

                             mid string comment '设备id',
                             os string comment '操作系统',
                             uid string comment '用户id',
                             vc string comment 'app版本',
                             page_id string comment '页面ID',
                             item string comment '页面item',
                             page_item_type string comment '页面item类型',
                             last_page_id string comment '上一页面ID',
                             source_type string comment '来源类型',
                             during_time string comment '页面停留时长',
                             url string comment '页面url',
                             ts string comment '时间戳'
)partitioned by (dt string);

insert into table  dwd_page_log

select
    get_json_object(line,"$.common.ar"),
    get_json_object(line,"$.common.ba"),
    get_json_object(line,"$.common.ch"),
    get_json_object(line,"$.common.is_new"),
    get_json_object(line,"$.common.md"),
    get_json_object(line,"$.common.mid"),
    get_json_object(line,"$.common.os"),
    get_json_object(line,"$.common.uid"),
    get_json_object(line,"$.common.vc"),
    get_json_object(line,"$.page.page_id"),
    get_json_object(line,"$.page.item"),
    get_json_object(line,"$.page.item_type"),
    get_json_object(line,"$.page.last_page_id"),
    get_json_object(line,"$.page.source_type"),
    get_json_object(line,"$.page.during_time"),
    CASE   -- 替换为实际字段名（如 good_detail、home 等）
        WHEN  get_json_object(line,"$.page.page_id")='good_detail' THEN 'https://example.com/detail/'
        WHEN  get_json_object(line,"$.page.page_id")='home' THEN 'https://example.com/home/'
        WHEN  get_json_object(line,"$.page.page_id")='payment' THEN 'https://example.com/payment/'
        WHEN  get_json_object(line,"$.page.page_id")='trade' THEN 'https://example.com/trade/'
        WHEN  get_json_object(line,"$.page.page_id")='good_list' THEN 'https://example.com/list/'
        WHEN  get_json_object(line,"$.page.page_id")='cart' THEN 'https://example.com/cart/'
        WHEN  get_json_object(line,"$.page.page_id")='search' THEN 'https://example.com/search/'
        WHEN  get_json_object(line,"$.page.page_id")='login' THEN 'https://example.com/login/'
        WHEN  get_json_object(line,"$.page.page_id")='register' THEN 'https://example.com/register/'
        WHEN  get_json_object(line,"$.page.page_id")='mine' THEN 'https://example.com/mine/'
        WHEN  get_json_object(line,"$.page.page_id")='orders_unpaid' THEN 'https://example.com/unpaid/'
        WHEN  get_json_object(line,"$.page.page_id")='good_spec' THEN 'https://example.com/spec/'
        WHEN  get_json_object(line,"$.page.page_id")='comment' THEN 'https://example.com/comment/'
        ELSE 'https://example.com/other/'  -- 默认情况
        END url,
    get_json_object(line,"$.ts"),
    from_unixtime(cast( get_json_object(line,"$.ts")/1000 as bigint),"yyyy-MM-dd")
from ods_log;
select * from dwd_page_log;


--TODO:-------------------dwd_action_log-------------------------

DROP TABLE IF EXISTS dwd_action_log;
CREATE EXTERNAL TABLE dwd_action_log(
                                        `area_code` STRING COMMENT '地区编码',
                                        `brand` STRING COMMENT '手机品牌',
                                        `channel` STRING COMMENT '渠道',
                                        `is_new` STRING COMMENT '是否首次启动',
                                        `model` STRING COMMENT '手机型号',
                                        `mid_id` STRING COMMENT '设备id',
                                        `os` STRING COMMENT '操作系统',
                                        `user_id` STRING COMMENT '会员id',
                                        `version_code` STRING COMMENT 'app版本号',
                                        `during_time` BIGINT COMMENT '持续时间毫秒',
                                        `page_item` STRING COMMENT '目标id ',
                                        `page_item_type` STRING COMMENT '目标类型',
                                        `last_page_id` STRING COMMENT '上页类型',
                                        `page_id` STRING COMMENT '页面id ',
                                        `source_type` STRING COMMENT '来源类型',
                                        `action_id` STRING COMMENT '动作id',
                                        `item` STRING COMMENT '目标id ',
                                        `item_type` STRING COMMENT '目标类型',
                                        `ts` BIGINT COMMENT '时间'
) COMMENT '动作日志表'
    PARTITIONED BY (`dt` STRING);




insert overwrite table dwd_action_log
select
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.is_new'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.page.during_time'),
    get_json_object(line,'$.page.item'),
    get_json_object(line,'$.page.item_type'),
    get_json_object(line,'$.page.last_page_id'),
    get_json_object(line,'$.page.page_id'),
    get_json_object(line,'$.page.source_type'),
    get_json_object(action,'$.action_id'),
    get_json_object(action,'$.item'),
    get_json_object(action,'$.item_type'),
    get_json_object(line,'$.ts'),
    from_unixtime(cast(get_json_object(line,'$.ts')/1000 as bigint),'yyyy-MM-dd')
from ods_log lateral view json_array_object(get_json_object(line,'$.actions')) action as action
where get_json_object(line,'$.actions') is not null;
select *
from dwd_action_log;


--tODO:----------------------dwd_display_log----------------------------------


DROP TABLE IF EXISTS dwd_display_log;
CREATE EXTERNAL TABLE dwd_display_log(
                                         `area_code` STRING COMMENT '地区编码',
                                         `brand` STRING COMMENT '手机品牌',
                                         `channel` STRING COMMENT '渠道',
                                         `is_new` STRING COMMENT '是否首次启动',
                                         `model` STRING COMMENT '手机型号',
                                         `mid_id` STRING COMMENT '设备id',
                                         `os` STRING COMMENT '操作系统',
                                         `user_id` STRING COMMENT '会员id',
                                         `version_code` STRING COMMENT 'app版本号',
                                         `during_time` BIGINT COMMENT 'app版本号',
                                         `page_item` STRING COMMENT '目标id ',
                                         `page_item_type` STRING COMMENT '目标类型',
                                         `last_page_id` STRING COMMENT '上页类型',
                                         `page_id` STRING COMMENT '页面ID ',
                                         `source_type` STRING COMMENT '来源类型',
                                         `ts` BIGINT COMMENT 'app版本号',
                                         `display_type` STRING COMMENT '曝光类型',
                                         `item` STRING COMMENT '曝光对象id ',
                                         `item_type` STRING COMMENT 'app版本号',
                                         `order` BIGINT COMMENT '曝光顺序',
                                         `pos_id` BIGINT COMMENT '曝光位置'
) COMMENT '曝光日志表'
    PARTITIONED BY (`dt` STRING);

insert overwrite table dwd_display_log
select
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.is_new'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.page.during_time'),
    get_json_object(line,'$.page.item'),
    get_json_object(line,'$.page.item_type'),
    get_json_object(line,'$.page.last_page_id'),
    get_json_object(line,'$.page.page_id'),
    get_json_object(line,'$.page.source_type'),
    get_json_object(line,'$.ts'),
    get_json_object(display,'$.display_type'),
    get_json_object(display,'$.item'),
    get_json_object(display,'$.item_type'),
    get_json_object(display,'$.order'),
    get_json_object(display,'$.pos_id'),
    from_unixtime(cast(get_json_object(line,'$.ts')/1000 as bigint) ,'yyyy-MM-dd')
from ods_log
         lateral view json_array_object(get_json_object(line,'$.displays')) display as display
where get_json_object(line,'$.displays') is not null;

select *
from dwd_display_log;

DROP TABLE IF EXISTS dwd_cart_info;
CREATE EXTERNAL TABLE dwd_cart_info(

                                       `user_id` STRING COMMENT '用户ID',
                                       `sku_id` STRING COMMENT '商品ID'
) COMMENT '加购事实表' PARTITIONED BY (`dt` STRING);

insert overwrite table dwd_cart_info partition(dt='2025-01-01')
select
    user_id,
    sku_id
from ods_cart_info
;
select *
from dwd_cart_info;


DROP TABLE IF EXISTS dwd_favor_info;
CREATE EXTERNAL TABLE dwd_favor_info(

                                        `sku_id` STRING  COMMENT 'skuid',
                                        `is_cancel` STRING  COMMENT '是否取消'

) COMMENT '收藏事实表'
    PARTITIONED BY (`dt` STRING);
insert overwrite table dwd_favor_info partition(dt='2025-01-01')
select
    sku_id,
    is_cancel
from ods_favor_info;
select *
from dwd_favor_info;



drop TABLE IF EXISTS dwd_keyword_search_volume;
CREATE EXTERNAL TABLE dwd_keyword_search_volume (
                                                    `keyword` STRING COMMENT '搜索关键词',
                                                    `search_count` STRING comment '搜索次数',
                                                    `category_id` string comment '商品品类ID'

) COMMENT '搜索业务表'
    PARTITIONED BY (`dt` STRING);

insert overwrite table dwd_keyword_search_volume partition(dt='2025-01-01')
select
    keyword,
    search_volume,
    category_id
from hot_search_keywords;


select * from dwd_keyword_search_volume;


--todo

-- dwd_order_info{
-- final_amount,order_status
-- }
create table if not exists dwd_order_info(
                                             `id`                     string comment '订单号',
                                             `final_amount`           decimal(16, 2) comment '订单最终金额',
                                             `order_status`           string comment '订单状态'
) comment '订单表'
    partitioned by (dt string)
    row format delimited fields terminated by "\t";



insert overwrite table dwd_order_info partition (dt)
select
    id,
    final_amount,
    order_status,
    date_format(create_time,"yyyy-MM-dd") dt
from ods_order_info;
select * from dwd_order_info;

-- dwd_order_detail {sku_id
-- , sku_num}


DROP TABLE IF EXISTS dwd_order_detail;
CREATE EXTERNAL TABLE dwd_order_detail (
                                           `id` STRING COMMENT '订单编号',
                                           `order_id` STRING COMMENT '订单号',
                                           `user_id` STRING COMMENT '用户id',
                                           `sku_id` STRING COMMENT 'sku商品id',
                                           `create_time` STRING COMMENT '创建时间',
                                           `sku_num` BIGINT COMMENT '商品数量',
                                           `original_amount` DECIMAL(16,2) COMMENT '原始价格',
                                           `split_final_amount` DECIMAL(16,2) COMMENT '最终价格分摊'
) COMMENT '订单明细事实表表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    TBLPROPERTIES ("parquet.compression"="lzo");
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_order_detail partition(dt)
select
    od.id,
    od.order_id,
    oi.user_id,
    od.sku_id,
    od.create_time,
    od.sku_num,
    od.order_price*od.sku_num,
    od.split_final_amount,
    date_format(create_time,'yyyy-MM-dd')
from
    (
        select
            *
        from ods_order_detail

    )od
        left join
    (
        select
            id,
            user_id
        from ods_order_info

    )oi;


select * from dwd_order_detail;




DROP TABLE IF EXISTS dwd_payment_info;
CREATE EXTERNAL TABLE dwd_payment_info (
                                           id STRING COMMENT '编号',
                                           order_id STRING COMMENT '订单编号',
                                           user_id STRING COMMENT '用户编号',
                                           create_time STRING COMMENT '创建时间',--调用第三方支付接口的时间
                                           callback_time STRING COMMENT '完成时间'--支付完成时间，即支付成功回调时间
) COMMENT '支付事实表表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    TBLPROPERTIES ("parquet.compression"="lzo");
--首日
SET hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_payment_info partition (dt)
select
    pi.id,
    pi.order_id,
    pi.user_id,
    pi.create_time,
    pi.callback_time,
    nvl(date_format(pi.callback_time,'yyyy-MM-dd HH:mm:ss'),'9999-99-99')
from (
         select
             *
         from ods_payment_info
     ) pi
         left join (
    select
        id
    from ods_order_info
) oi
                   on pi.order_id = oi.id;

select * from dwd_payment_info;



DROP TABLE IF EXISTS dwd_payment_info;
CREATE EXTERNAL TABLE dwd_payment_info (
                                           id STRING COMMENT '编号',
                                           order_id STRING COMMENT '订单编号',
                                           user_id STRING COMMENT '用户编号',
                                           create_time STRING COMMENT '创建时间',--调用第三方支付接口的时间
                                           callback_time STRING COMMENT '完成时间'--支付完成时间，即支付成功回调时间
) COMMENT '支付事实表表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
--首日
SET hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_payment_info partition (dt)
select
    pi.id,
    pi.order_id,
    pi.user_id,
    pi.create_time,
    pi.callback_time,
    nvl(date_format(pi.callback_time,'yyyy-MM-dd'),'9999-99-99')
from (
         select
             *
         from ods_payment_info
     ) pi
         left join (
    select
        id
    from ods_order_info
) oi
                   on pi.order_id = oi.id;

-- load data local inpath '/opt/module/data/data.txt' into table dwd_payment_info partition (dt = '2025-01-01');

select * from dwd_payment_info;