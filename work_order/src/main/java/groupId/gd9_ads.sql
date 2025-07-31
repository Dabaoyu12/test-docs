set hive.exec.mode.local.auto=true;
create database if not exists gd_4and9;
use gd_4and9;

--------------------------------------------工单9_1--------------------------------------------
drop table ads9_1;
create table ads9_1 (
                             recent_days string,   ----时间维度
                             trade_page_id string, ----店铺页
                             trade_count string, ---- 店铺数访客数
                             trade_ratio string, ----店铺页占比率
                             trade_count_payment string, ----店铺页支付人数
                             trade_payment_ratio string, ----店铺页支付占比率
                             trade_payment_ratio_2 string, ----店铺下单转化率
                             good_detail_page_id string, ----商品详情页
                             detail_count string, ----商品详情页访客数
                             detail_ratio string, ----商品详情页占比率
                             detail_count_payment string, -----商品详情页支付人数
                             detail_payment_ratio string, ----商品详情页支付占比率
                             detail_payment_ratio_2 string, ----商品详情页支付转化率
                             other_page_id string, ----其他页
                             other_sum string, ----其他页访客数
                             other_ratio string, ----其他页占比率
                             other_count_payment string, ----其他页支付人数
                             other_payment_ratio string, ----其他页支付占比率
                             other_payment_ratio_2 string ----其他页支付转化率
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

with t0 as ( select page_id,dt,recent_days,mid,ts from dwd_page_log lateral view explode (array(1,7,30))tmp as recent_days ),
     t1 as ( select page_id,
                    recent_days,
                    count(*) trade_count
             from t0
             where page_id="trade"
               and "2025-01-07" >= date_add(dt,-recent_days+1)
             group by page_id,recent_days),
     t2 as ( select
                 page_id,
                 recent_days,
                 count(*) detail_count
             from t0 where
                     page_id = "good_detail"
                       and "2025-01-07" >= date_add(dt,-recent_days+1)
             group by page_id,recent_days),
     t3 as (
         select
             page_id,
             recent_days,
             sum(other_count) other_sum
         from (
                  select
                      "other" as page_id,
                      recent_days,
                      count(*) other_count
                  from t0 where
                          page_id in ("spec","list","comment")
                            and "2025-01-07" >= date_add(dt,-recent_days+1)
                  group by page_id,recent_days
              ) tmp
         group by page_id,recent_days
     ),
     t4 as (
         select
             t1.page_id    trade_page_id,
             trade_count    ,
             t2.page_id      good_detail_page_id,
             detail_count ,
             t3.page_id   other_page_id,
             other_sum,
             t1.recent_days as recent_days,
             trade_count+detail_count+other_sum as count
         from t1,t2,t3
     ),t5 as (
    select
        recent_days,
        trade_page_id,
        trade_count,
        trade_count/count as trade_ratio,
        good_detail_page_id,
        detail_count,
        detail_count/count as detail_ratio,
        other_page_id,
        other_sum,
        other_sum/count as other_ratio
    from t4
),t6 as(
    SELECT
        count(*) as trade_count_payment
    FROM (
             SELECT
                 mid,
                 ts,
                 dt,
                 page_id,
                 recent_days,
                 LEAD(page_id, 1) OVER (PARTITION BY mid ORDER BY ts) AS next_page_id
             FROM dwd_page_log
                      lateral view explode (array(1,7,30))tmp as recent_days
             where "2025-01-07" >= date_add(dt,-recent_days+1)
         ) tmp1
    WHERE page_id = 'trade'
      AND next_page_id = 'payment'
),t7 as(
    SELECT
        count(*) as detail_count_payment
    FROM (
             SELECT
                 mid,
                 ts,
                 dt,
                 page_id,
                 recent_days,
                 LEAD(page_id, 1) OVER (PARTITION BY mid ORDER BY ts) AS next_page_id
             FROM dwd_page_log
                      lateral view explode (array(1,7,30))tmp as recent_days
             where "2025-01-07" >= date_add(dt,-recent_days+1)
         ) tmp1
    WHERE page_id = 'good_detail'
      AND next_page_id = 'payment'
),t8 as(
    SELECT
        count(*) as other_count_payment
    FROM (
             SELECT
                 mid,
                 ts,
                 dt,
                 page_id,
                 recent_days,
                 LEAD(page_id, 1) OVER (PARTITION BY mid ORDER BY ts) AS next_page_id
             FROM dwd_page_log
                      lateral view explode (array(1,7,30))tmp as recent_days
             where "2025-01-07" >= date_add(dt,-recent_days+1)
         ) tmp1
    WHERE page_id in ("spec","list","comment")
      AND next_page_id = 'payment'
),t9 as (   select
                trade_count_payment,
                detail_count_payment,
                other_count_payment,
                trade_count_payment+detail_count_payment+other_count_payment as payment_count
            from t6,t7,t8)
insert overwrite table ads9_1
select
    recent_days,   ----时间维度
    trade_page_id, ----店铺页
    trade_count, ---- 店铺数访客数
    CONCAT(Round(trade_ratio * 100, 2), '%') AS trade_ratio_percentag, -----店铺页占比率
    trade_count_payment, ----店铺页支付人数
    CONCAT(Round(trade_count_payment/payment_count * 100, 2), '%') AS trade_ratio_percentag, ----店铺页支付占比率
    CONCAT(Round(trade_count_payment/trade_count * 100, 2), '%') AS trade_ratio_percentag,----店铺下单转化率
    good_detail_page_id, ----商品详情页
    detail_count, ----商品详情页访客数
    CONCAT(Round(detail_ratio * 100, 2), '%') AS detail_ratio,----店铺下单转化率
    detail_count_payment, -----商品详情页支付人数
    CONCAT(Round(detail_count_payment/payment_count * 100, 2), '%') AS detail_payment_ratio,----商品详情页支付占比率
    CONCAT(Round(detail_count_payment/detail_count * 100, 2), '%') AS detail_payment_ratio_2,----商品详情页支付转化率
    other_page_id, ----其他页
    other_sum, ----其他页访客数
    CONCAT(Round(other_ratio * 100, 2), '%') AS other_ratio,----其他页占比率
    other_count_payment, ----其他页支付人数
    CONCAT(Round(other_count_payment/payment_count * 100, 2), '%') AS other_payment_ratio,----其他页支付占比率
    CONCAT(Round(other_count_payment/other_sum * 100, 2), '%') AS other_payment_ratio_2----其他页支付转化率
from t5,t9;

select * from ads9_1;

--------------------------------------------工单9_2--------------------------------------------
drop table if exists ads9_2;
CREATE TABLE IF NOT EXISTS ads9_2 (
                                                     rank INT COMMENT '页面类型排名，按独立访客数(UV)从高到低排序',
                                                     page_type STRING COMMENT '页面分类类型：店铺页(包含home/search/good_list/cart/trade)、商品详情页(good_detail)、店铺其他页(其他页面)',
                                                     pv BIGINT COMMENT '页面浏览量(Page View)，该类型页面的总访问次数',
                                                     uv BIGINT COMMENT '独立访客数(Unique Visitor)，访问该类型页面的去重用户数',
                                                     avg_stay_duration_seconds DECIMAL(10,2) COMMENT '平均停留时长(秒)，用户在该类型页面的平均停留时间，由毫秒转换而来'
);
WITH
    page_classification AS (
        SELECT
            page_id,
            CASE
                WHEN page_id IN ('home', 'search', 'good_list', 'cart', 'trade') THEN '店铺页'
                WHEN page_id = 'good_detail' THEN '商品详情页'
                ELSE '店铺其他页'
                END AS page_type
        FROM (
                 SELECT
                     page_id,
                     page_item_type,
                     ROW_NUMBER() OVER (PARTITION BY page_id ORDER BY ts DESC) AS rn
                 FROM dwd_page_log  -- 假设表名为page_log_table，根据您的实际表名调整
                 WHERE dt = '2025-01-07'
             ) t
        WHERE rn = 1  -- 取每个页面的最新类型
    ),

-- 计算各页面类型的流量指标
    page_metrics AS (
        SELECT
            pc.page_type,
            COUNT(1) AS pv, -- 浏览量
            COUNT(DISTINCT mid) AS uv, -- 访客数
            AVG(during_time) AS avg_stay_duration -- 平均停留时长(毫秒)
        FROM dwd_page_log pl
                 JOIN page_classification pc ON pl.page_id = pc.page_id
        WHERE pl.dt = '2025-01-07'
        GROUP BY pc.page_type
    )

insert into table ads9_2
-- 最终结果按访客数排序
SELECT
    row_number() OVER (ORDER BY uv DESC) AS rank,
    page_type,
    pv ,
    uv,
    ROUND(avg_stay_duration/1000, 2)
FROM page_metrics;

select * from ads9_2;

--------------------------------------------工单9_3--------------------------------------------
drop table if exists ads9_3;
CREATE TABLE IF NOT EXISTS ads9_3 (
                                                    source_page STRING COMMENT '来源页面',
                                                    target_page STRING COMMENT '目标页面',
                                                    visitor_count BIGINT COMMENT '访客数量',
                                                    visitor_percentage DECIMAL(5,2) COMMENT '访客占比(%)',
                                                    total_payment_amount DECIMAL(16,2) COMMENT '总支付金额',
                                                    payment_percentage DECIMAL(5,2) COMMENT '支付金额占比(%)'
);

with page_path as (
    -- 提取每个访客的页面访问路径（来源 -> 去向）
    select
        mid,
        uid,
        last_page_id as source_page,  -- 来源页面
        page_id as target_page,      -- 去向页面
        ts as visit_time             -- 访问时间戳
    from
        dwd_page_log
    where
            dt = '2025-01-07'
      and last_page_id is not null -- 排除首次进入的页面
),

-- 获取每个用户在当天的总支付金额
     user_payment as (
         select
             user_id,
             sum(create_time) as total_payment_amount
         from
             dwd_payment_info
         where
                 dt = '2025-01-07'
         group by
             user_id
     ),

-- 将页面流转路径和支付金额关联
     path_with_payment as (
         select
             pp.source_page,
             pp.target_page,
             pp.mid,
             pp.uid,
             coalesce(up.total_payment_amount, 0) as payment_amount
         from
             page_path pp
                 left join
             user_payment up
             on pp.uid = up.user_id
     ),

-- 按照来源和去向统计指标
     path_stats as (
         select
             source_page,
             target_page,
             count(distinct mid) as visitor_count,
             sum(payment_amount) as total_payment_amount
         from
             path_with_payment
         group by
             source_page, target_page
     ),

-- 计算整体总量用于占比计算
     total_stats as (
         select
             sum(visitor_count) as total_visitors,
             sum(total_payment_amount) as total_payments
         from
             path_stats
     )

insert into table ads9_3

-- 最终结果：展示来源、去向、访客数、支付金额及各自占比
select
    ps.source_page,
    ps.target_page,
    ps.visitor_count,
    round((ps.visitor_count / ts.total_visitors) * 100, 2) as visitor_percentage,
    ps.total_payment_amount,
    round((ps.total_payment_amount / ts.total_payments) * 100, 2) as payment_percentage
from
    path_stats ps
        cross join
    total_stats ts
order by
    visitor_percentage desc;

select * from ads9_3;

--------------------------------------------工单9_4--------------------------------------------
drop table if exists ads9_4;
CREATE TABLE IF NOT EXISTS ads9_4 (
                                                       ch STRING COMMENT '访问渠道，未知渠道会标记为"未知渠道"',
                                                       uv BIGINT COMMENT '独立访客数(COUNT DISTINCT mid)',
                                                       bounce_rate DECIMAL(5,2) COMMENT '跳出率百分比(ROUND(SUM(CASE WHEN last_page_id IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(DISTINCT mid),2))'
);

insert into table ads9_4
SELECT
    COALESCE(ch, '未知渠道'),
    COUNT(DISTINCT uid) AS c1,
    ROUND(SUM(CASE WHEN last_page_id IS NULL THEN 1 ELSE 0 END) * 100.0 /
          COUNT(DISTINCT uid), 2) AS c2
FROM dwd_page_log
WHERE dt = '2025-01-07'
GROUP BY ch
ORDER BY c2 DESC
LIMIT 20;

select * from ads9_4;

--------------------------------------------工单9_5--------------------------------------------
drop table if exists ads9_5;
create table ads9_5(
                            url string,
                            pv string,
                            uv string,
                            during_time string
);


with   aa as (
    select
        url,--url ,
        avg (during_time) during_time,
        count(     distinct  mid) uv
    FROM dwd_page_log where  page_id is not null group by url

),bb as (

    select
        url,

        count(   *) pv
    FROM dwd_page_log page_id where page_id is not null  group by url
) insert into table ads9_5

select

    aa.url,
    bb.pv,
    aa.uv,
    round( during_time/1000,2) during_time
from aa left join bb on aa.url=bb.url order by pv desc;

select * from ads9_5;

--------------------------------------------工单9_6--------------------------------------------
CREATE TABLE IF NOT EXISTS ads9_6 (
                                                        source_page STRING COMMENT '来源页面ID或名称',
                                                        target_page STRING COMMENT '目标页面ID或名称',
                                                        visitor_count BIGINT COMMENT '从来源页到目标页的独立访客数',
                                                        visitor_percentage DECIMAL(5,2) COMMENT '访客占比(占总访客的百分比)',
                                                        total_payment_amount DECIMAL(16,2) COMMENT '从来源页到目标页产生的总支付金额',
                                                        payment_percentage DECIMAL(5,2) COMMENT '支付金额占比(占总支付金额的百分比)'
);


with page_path as (
    -- 提取每个访客的页面访问路径（来源 -> 去向）
    select
        mid,
        uid,
        last_page_id as source_page,  -- 来源页面
        page_id as target_page,      -- 去向页面
        ts as visit_time             -- 访问时间戳
    from
        dwd_page_log
    where
            dt = '2025-01-07'
      and last_page_id is not null -- 排除首次进入的页面
      AND last_page_id NOT IN ('home', 'search', 'good_list', 'cart', 'trade', 'good_detail')
),

-- 获取每个用户在当天的总支付金额
     user_payment as (
         select
             user_id,
             sum(create_time) as total_payment_amount
         from
             dwd_payment_info
         where
                 dt = '2025-01-07'
         group by
             user_id
     ),

-- 将页面流转路径和支付金额关联
     path_with_payment as (
         select
             pp.source_page,
             pp.target_page,
             pp.mid,
             pp.uid,
             coalesce(up.total_payment_amount, 0) as payment_amount
         from
             page_path pp
                 left join
             user_payment up
             on pp.uid = up.user_id
     ),

-- 按照来源和去向统计指标
     path_stats as (
         select
             source_page,
             target_page,
             count(distinct mid) as visitor_count,
             sum(payment_amount) as total_payment_amount
         from
             path_with_payment
         group by
             source_page, target_page
     ),

-- 计算整体总量用于占比计算
     total_stats as (
         select
             sum(visitor_count) as total_visitors,
             sum(total_payment_amount) as total_payments
         from
             path_stats
     )

insert into table ads9_6

-- 最终结果：展示来源、去向、访客数、支付金额及各自占比
select
    ps.source_page,
    ps.target_page,
    ps.visitor_count,
    round((ps.visitor_count / ts.total_visitors) * 100, 2) as visitor_percentage,
    ps.total_payment_amount,
    round((ps.total_payment_amount / ts.total_payments) * 100, 2) as payment_percentage
from
    path_stats ps
        cross join
    total_stats ts
order by
    visitor_percentage desc;


select * from ads9_6;


