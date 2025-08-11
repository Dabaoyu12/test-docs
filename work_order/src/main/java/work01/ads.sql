USE gmall_work_01;

-- 1. 商品全景汇总表
CREATE TABLE ads_item_overview_dd (
    date_key INT COMMENT '日期键YYYYMMDD',
    item_id BIGINT COMMENT '商品ID',
    category_id INT COMMENT '叶子类目ID',
    -- 基础指标
    uv BIGINT COMMENT '商品访客数',
    pv BIGINT COMMENT '商品浏览量',
    visited_items BIGINT COMMENT '有访问商品数',
    total_dwell_sec BIGINT COMMENT '总停留时长(秒)',
    bounce_count BIGINT COMMENT '跳出人数',
    micro_uv BIGINT COMMENT '微详情访客数',
    -- 转化指标
    fav_uv BIGINT COMMENT '收藏人数',
    cart_qty BIGINT COMMENT '加购件数',
    cart_uv BIGINT COMMENT '加购人数',
    order_uv BIGINT COMMENT '下单买家数',
    pay_uv BIGINT COMMENT '支付买家数',
    -- 价值指标
    order_qty BIGINT COMMENT '下单件数',
    order_amt DECIMAL(18,2) COMMENT '下单金额',
    pay_qty BIGINT COMMENT '支付件数',
    pay_amt DECIMAL(18,2) COMMENT '支付金额',
    paid_items BIGINT COMMENT '有支付商品数',
    refund_amt DECIMAL(18,2) COMMENT '退款金额',
    jhs_pay_amt DECIMAL(18,2) COMMENT '聚划算支付金额',
    ytd_pay_amt DECIMAL(18,2) COMMENT '年累计支付金额',
    -- 计算字段
    avg_stay_sec DECIMAL(10,2) COMMENT '平均停留时长(秒)',
    bounce_rate DECIMAL(5,4) COMMENT '跳出率',
    fav_conversion_rate DECIMAL(5,4) COMMENT '收藏转化率',
    cart_conversion_rate DECIMAL(5,4) COMMENT '加购转化率',
    order_conversion_rate DECIMAL(5,4) COMMENT '下单转化率',
    pay_conversion_rate DECIMAL(5,4) COMMENT '支付转化率',
    atv DECIMAL(10,2) COMMENT '客单价',
    visitor_value DECIMAL(10,2) COMMENT '访客平均价值',
    etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'ADS-商品全景日汇总表（支撑效率监控看板）'
    STORED AS ORC;

-- 2. 商品分层聚合表
CREATE TABLE ads_item_interval_dd (
    date_key INT COMMENT '日期键YYYYMMDD',
    category_id INT COMMENT '叶子类目ID',
    price_band STRING COMMENT '价格带区间(如0-50)',
    pay_qty_band STRING COMMENT '支付件数区间(如0-10)',
    pay_amt_band STRING COMMENT '支付金额区间(如0-100)',
    -- 核心指标
    sale_item_count BIGINT COMMENT '动销商品数',
    total_pay_amt DECIMAL(18,2) COMMENT '支付金额',
    total_pay_qty BIGINT COMMENT '支付件数',
    new_buyer_count BIGINT COMMENT '支付新买家数',
    new_buyer_amt DECIMAL(18,2) COMMENT '新买家支付金额',
    -- 计算字段
    avg_item_price DECIMAL(18,2) COMMENT '件单价',
    etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'ADS-商品分层聚合表（支撑区间分析看板）'
    STORED AS ORC;

-- 3. 新老买家分析表
CREATE TABLE ads_item_buyer_type_dd (
      date_key INT COMMENT '日期键YYYYMMDD',
      item_id BIGINT COMMENT '商品ID',
    -- 买家类型指标
      new_buyer_count BIGINT COMMENT '支付新买家数',
      old_buyer_count BIGINT COMMENT '支付老买家数',
      new_buyer_amt DECIMAL(18,2) COMMENT '新买家支付金额',
      old_buyer_amt DECIMAL(18,2) COMMENT '老买家支付金额',
      etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'ADS-商品新老买家分析表'
    STORED AS ORC;

-- 1. 商品全景汇总表ETL
INSERT INTO ads_item_overview_dd
SELECT
    d.date_key,
    d.item_id,
    i.category_id,
    -- 基础指标
    d.visitor_count AS uv,
    d.visit_count AS pv,
    CASE WHEN d.visitor_count > 0 THEN 1 ELSE 0 END AS visited_items,
    d.total_dwell_seconds AS total_dwell_sec,
    d.bounce_count,
    d.micro_detail_user_count AS micro_uv,
    -- 转化指标
    d.fav_add_count AS fav_uv,
    d.cart_add_quantity AS cart_qty,
    cart.cart_uv,
    d.order_user_count AS order_uv,
    d.payment_user_count AS pay_uv,
    -- 价值指标
    d.order_quantity AS order_qty,
    d.order_amount AS order_amt,
    d.payment_quantity AS pay_qty,
    d.payment_amount AS pay_amt,
    CASE WHEN d.payment_quantity > 0 THEN 1 ELSE 0 END AS paid_items,
    COALESCE(r.refund_amt, 0) AS refund_amt,
    COALESCE(jhs.jhs_pay_amt, 0) AS jhs_pay_amt,
    -- 年累计支付金额
    COALESCE(ytd.ytd_pay_amt, 0) AS ytd_pay_amt,
    -- 计算字段
    ROUND(d.total_dwell_seconds / NULLIF(d.visitor_count, 0), 2) AS avg_stay_sec,
    ROUND(d.bounce_count / NULLIF(d.visitor_count, 0), 4) AS bounce_rate,
    ROUND(d.fav_add_count / NULLIF(d.visitor_count, 0), 4) AS fav_conversion_rate,
    ROUND(cart.cart_uv / NULLIF(d.visitor_count, 0), 4) AS cart_conversion_rate,
    ROUND(d.order_user_count / NULLIF(d.visitor_count, 0), 4) AS order_conversion_rate,
    ROUND(d.payment_user_count / NULLIF(d.visitor_count, 0), 4) AS pay_conversion_rate,
    ROUND(d.payment_amount / NULLIF(d.payment_user_count, 0), 2) AS atv,
    ROUND(d.payment_amount / NULLIF(d.visitor_count, 0), 2) AS visitor_value,
    CURRENT_TIMESTAMP()
FROM dws_item_action_dd d
         JOIN dim_item i ON d.item_id = i.item_id
         JOIN dim_date dd ON d.date_key = dd.date_key
-- 加购人数子查询
         LEFT JOIN (
    SELECT date_key, item_id, COUNT(DISTINCT user_id) AS cart_uv
    FROM dwd_cart_action_di
    WHERE action_type = 'ADD'
    GROUP BY date_key, item_id
) cart ON d.date_key = cart.date_key AND d.item_id = cart.item_id
-- 退款金额子查询
         LEFT JOIN (
    SELECT item_id, date_key, SUM(refund_amount) AS refund_amt
    FROM ods_event_refund
    GROUP BY item_id, date_key
) r ON d.item_id = r.item_id AND d.date_key = r.date_key
-- 聚划算支付金额子查询
         LEFT JOIN (
    SELECT item_id, date_key, SUM(payment_amount) AS jhs_pay_amt
    FROM dwd_payment_di
    WHERE activity_id = 'JUHUASUAN'
    GROUP BY item_id, date_key
) jhs ON d.item_id = jhs.item_id AND d.date_key = jhs.date_key
-- 年累计支付金额子查询
         LEFT JOIN (
    SELECT
        y.item_id,
        d.date_key,
        SUM(y.payment_amount) OVER (
            PARTITION BY y.item_id, YEAR(dd.date_value)
            ORDER BY d.date_key
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS ytd_pay_amt
    FROM dws_item_action_dd y
             JOIN dim_date dd ON y.date_key = dd.date_key
) ytd ON d.item_id = ytd.item_id AND d.date_key = ytd.date_key;

-- 2. 商品分层聚合表ETL
INSERT INTO ads_item_interval_dd
SELECT
    date_key,
    category_id,
    price_band,
    pay_qty_band,
    pay_amt_band,
    COUNT(DISTINCT item_id) AS sale_item_count,
    SUM(pay_amt) AS total_pay_amt,
    SUM(pay_qty) AS total_pay_qty,
    SUM(new_buyer_count) AS new_buyer_count,
    SUM(new_buyer_amt) AS new_buyer_amt,
    SUM(pay_amt) / NULLIF(SUM(pay_qty), 0) AS avg_item_price,
    CURRENT_TIMESTAMP()
FROM (
         SELECT
             d.date_key,
             i.category_id,
             d.item_id,
             d.pay_qty,
             d.pay_amt,
             COALESCE(b.new_buyer_count, 0) AS new_buyer_count,
             COALESCE(b.new_buyer_amt, 0) AS new_buyer_amt,
             CASE
                 WHEN i.price <= 50 THEN '0-50'
                 WHEN i.price <= 100 THEN '51-100'
                 WHEN i.price <= 200 THEN '101-200'
                 ELSE '201+'
                 END AS price_band,
             CASE
                 WHEN d.pay_qty <= 10 THEN '0-10'
                 WHEN d.pay_qty <= 50 THEN '11-50'
                 ELSE '51+'
                 END AS pay_qty_band,
             CASE
                 WHEN d.pay_amt <= 100 THEN '0-100'
                 WHEN d.pay_amt <= 500 THEN '101-500'
                 ELSE '501+'
                 END AS pay_amt_band
         FROM ads_item_overview_dd d
                  JOIN dim_item i ON d.item_id = i.item_id
                  LEFT JOIN ads_item_buyer_type_dd b
                            ON d.date_key = b.date_key AND d.item_id = b.item_id
         WHERE d.pay_qty > 0
     ) AS item_stats
GROUP BY date_key, category_id, price_band, pay_qty_band, pay_amt_band;

-- 3. 新老买家分析表ETL
INSERT INTO ads_item_buyer_type_dd
SELECT
    p.date_key,
    p.item_id,
    -- 新买家数量
    SUM(CASE
            WHEN u.last_payment_date IS NULL
                OR u.last_payment_date < DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(p.date_key AS STRING), 'yyyyMMdd')), 365)
                THEN 1
            ELSE 0
        END) AS new_buyer_count,
    -- 老买家数量
    SUM(CASE
            WHEN u.last_payment_date IS NOT NULL
                AND u.last_payment_date >= DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(p.date_key AS STRING), 'yyyyMMdd')), 365)
                THEN 1
            ELSE 0
        END) AS old_buyer_count,
    -- 新买家支付金额
    SUM(CASE
            WHEN u.last_payment_date IS NULL
                OR u.last_payment_date < DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(p.date_key AS STRING), 'yyyyMMdd')), 365)
                THEN p.payment_amount
            ELSE 0
        END) AS new_buyer_amt,
    -- 老买家支付金额
    SUM(CASE
            WHEN u.last_payment_date IS NOT NULL
                AND u.last_payment_date >= DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(p.date_key AS STRING), 'yyyyMMdd')), 365)
                THEN p.payment_amount
            ELSE 0
        END) AS old_buyer_amt,
    CURRENT_TIMESTAMP()
FROM dwd_payment_di p
         JOIN dim_user u ON p.user_id = u.user_id
GROUP BY p.date_key, p.item_id;