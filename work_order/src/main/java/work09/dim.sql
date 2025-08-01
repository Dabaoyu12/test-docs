set hive.exec.mode.local.auto=true;
create database if not exists gd_4and9;
use gd_4and9;

DROP TABLE IF EXISTS dim_sku_info;
CREATE EXTERNAL TABLE dim_sku_info (
                                       `id` STRING COMMENT '商品id',
                                       `price` DECIMAL(16,2) COMMENT '商品价格',
                                       `sku_name` STRING COMMENT '商品名称',
                                       `spu_id` STRING COMMENT 'spu编号',
                                       `spu_name` STRING COMMENT 'spu名称',
                                       `category3_id` STRING COMMENT '三级分类id',
                                       `category3_name` STRING COMMENT '三级分类名称',
                                       `category2_id` STRING COMMENT '二级分类id',
                                       `category2_name` STRING COMMENT '二级分类名称',
                                       `category1_id` STRING COMMENT '一级分类id',
                                       `category1_name` STRING COMMENT '一级分类名称',
                                       `keyword` STRING COMMENT '搜索词',
                                       `category_id` STRING COMMENT '分类id',
                                       `user_id` STRING COMMENT '用户id',
                                       `create_time` STRING COMMENT '创建时间'
) COMMENT '商品维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    TBLPROPERTIES ("parquet.compression"="lzo");

with
    sku as
        (
            select
                id,
                price,
                sku_name,
                spu_id,
                category3_id,
                create_time
            from ods_sku_info

        ),
    spu as
        (
            select
                id,
                spu_name
            from ods_spu_info

        ),
    c3 as
        (
            select
                id,
                name,
                category2_id
            from ods_base_category3

        ),
    c2 as
        (
            select
                id,
                name,
                category1_id
            from ods_base_category2

        ),
    c1 as
        (
            select
                id,
                name
            from ods_base_category1

        ),hos as (select keyword,category_id
                  from hot_search_keywords
),odi as (
    select  sku_id, user_id from ods_order_detail ood left join ods_order_info  oi on ood.order_id=oi.id
)

insert overwrite table dim_sku_info partition(dt = "2025-01-01")
select
    sku.id,
    sku.price,
    sku.sku_name,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name,
    c3.category2_id,
    c2.name,
    c2.category1_id,
    c1.name,
    hos.keyword,
    hos.category_id,
    odi.user_id,
    sku.create_time
from sku
         left join spu on sku.spu_id=spu.id
         left join c3 on sku.category3_id=c3.id
         left join c2 on c3.category2_id=c2.id
         left join c1 on c2.category1_id=c1.id
         LEFT JOIN hos ON c1.id=hos.category_id
         left join odi on sku.id=odi.sku_id;

select * from dim_sku_info;



-- create table dim_ss  as
with kk  as ( select  uid, item sku_id,dt from dwd_page_log where page_item_type='sku_id') ,

     bb as (
         select  uid, item   `rsc`,dt
         from   dwd_page_log where page_item_type="keyword" and last_page_id="search"
     )

select
            sku_id,
            `rsc`  as keyword,
            count(*) num

from kk  join  bb on kk.uid=bb.uid and kk.dt=bb.dt where bb.dt="2025-01-01" group by sku_id,`rsc`;

