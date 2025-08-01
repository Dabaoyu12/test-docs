create database if not exists gmall_work_07;
use gmall_work_07;

-- 创建辅助数字表 (1-1000)
CREATE TEMPORARY TABLE numbers (n INT);
INSERT INTO numbers (n)
SELECT a.N + b.N * 10 + c.N * 100
FROM
    (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
    (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
    (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c
LIMIT 1000;

CREATE TEMPORARY TABLE numbers (n INT);
INSERT INTO numbers (n)
SELECT a.N + b.N * 10 + c.N * 100
FROM
    (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
    (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
    (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c
LIMIT 1000;

-- 1. 修正后的商品维度表 (ods_dim_product)
INSERT INTO ods_dim_product (
    product_name, category_id, category_name, brand_id, brand_name,
    launch_date, attributes, status
)
SELECT
    CONCAT('Product ', n) AS product_name,
    FLOOR(RAND() * 100) + 1 AS category_id,
    CONCAT('Category ', FLOOR(RAND() * 10)) AS category_name,
    FLOOR(RAND() * 50) + 1 AS brand_id,
    CONCAT('Brand ', FLOOR(RAND() * 20)) AS brand_name,
    DATE_ADD('2020-01-01', INTERVAL FLOOR(RAND() * 1800) DAY) AS launch_date,
    JSON_OBJECT(
            'color', ELT(FLOOR(RAND() * 5) + 1, 'Red','Blue','Green','Black','White'),
            'material', ELT(FLOOR(RAND() * 4) + 1, 'Cotton','Polyester','Wool','Silk')
    ) AS attributes,
    ELT(FLOOR(RAND() * 2) + 1, 'ON','OFF') AS status
FROM numbers;

-- 2. SKU维度表 (ods_dim_sku)
INSERT INTO ods_dim_sku (
    product_id, sku_code, color, size, packaging,
    weight, price, status
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS product_id,
    CONCAT('SKU', n) AS sku_code,
    ELT(FLOOR(RAND() * 5) + 1, 'Red','Blue','Green','Black','White') AS color,
    CONCAT(FLOOR(RAND() * 50) + 1, '-', FLOOR(RAND() * 10) + 1) AS size,
    ELT(FLOOR(RAND() * 4) + 1, 'Box','Bag','Wrap','None') AS packaging,
    ROUND(RAND() * 10, 3) AS weight,
    ROUND(10 + RAND() * 100, 2) AS price,
    ELT(FLOOR(RAND() * 2) + 1, 'ON','OFF') AS status
FROM numbers;

-- 3. 供应商维度表 (ods_dim_vendor) - 保持不变
INSERT INTO ods_dim_vendor (
    vendor_name, vendor_level, rating_score, contact_info
)
SELECT
    CONCAT('Vendor ', n) AS vendor_name,
    ELT(FLOOR(RAND() * 3) + 1, 'A','B','C') AS vendor_level,
    ROUND(1 + RAND() * 4, 2) AS rating_score,
    JSON_OBJECT('phone', CONCAT('1', FLOOR(RAND() * 1000000000)),
                'email', CONCAT('contact', n, '@vendor.com'))
FROM numbers;

-- 4. 日期维度表 (ods_dim_date) - 保持不变
DROP TEMPORARY TABLE IF EXISTS numbers;
CREATE TEMPORARY TABLE numbers (n INT);
INSERT INTO numbers (n)
SELECT a.N + b.N * 10 + c.N * 100
FROM
    (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
    (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
    (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c
LIMIT 1000;

-- 4. 日期维度表 (ods_dim_date) - 安全插入
INSERT IGNORE INTO ods_dim_date (
    date_key, day_of_week, day_of_month, month,
    quarter, year, is_weekend
)
SELECT
    DATE_ADD('2020-01-01', INTERVAL n DAY) AS date_key,
    DAYOFWEEK(DATE_ADD('2020-01-01', INTERVAL n DAY)) AS day_of_week,
    DAYOFMONTH(DATE_ADD('2020-01-01', INTERVAL n DAY)) AS day_of_month,
    MONTH(DATE_ADD('2020-01-01', INTERVAL n DAY)) AS month,
    QUARTER(DATE_ADD('2020-01-01', INTERVAL n DAY)) AS quarter,
    YEAR(DATE_ADD('2020-01-01', INTERVAL n DAY)) AS year,
    DAYOFWEEK(DATE_ADD('2020-01-01', INTERVAL n DAY)) IN (1,7) AS is_weekend
FROM numbers
WHERE n < 1000;

-- 5. 用户维度表 (ods_dim_user)
INSERT INTO ods_dim_user (
    register_time, register_channel, user_level, city, age_group
)
SELECT
    DATE_ADD('2020-01-01', INTERVAL FLOOR(RAND() * 1800) DAY) AS register_time,
    ELT(FLOOR(RAND() * 4) + 1, 'Web','App','WeChat','Offline') AS register_channel,
    ELT(FLOOR(RAND() * 3) + 1, 'NEW','ACTIVE','VIP') AS user_level,
    ELT(FLOOR(RAND() * 10) + 1, 'Beijing','Shanghai','Guangzhou','Shenzhen','Chengdu','Hangzhou','Wuhan','Nanjing','Xi\'an','Chongqing') AS city,
    ELT(FLOOR(RAND() * 5) + 1, '<18','18-25','26-35','36-45','>45') AS age_group
FROM numbers;

-- 6. 竞品商品维度表 (ods_dim_competitor_product)
INSERT INTO ods_dim_competitor_product (
    comp_name, comp_category_id, comp_category_name,
    comp_price, comp_brand
)
SELECT
    CONCAT('Competitor ', n) AS comp_name,
    FLOOR(RAND() * 100) + 1 AS comp_category_id,
    CONCAT('Comp_Category ', FLOOR(RAND() * 10)) AS comp_category_name,
    ROUND(50 + RAND() * 200, 2) AS comp_price,
    CONCAT('Comp_Brand ', FLOOR(RAND() * 20)) AS comp_brand
FROM numbers;

-- 7. 页面浏览日志表 (ods_fact_pageview)
INSERT INTO ods_fact_pageview (
    user_id, product_id, session_id, page_url,
    referer, visit_time, device_type, browser,
    city, date_key
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS user_id,
    FLOOR(RAND() * 1000) + 1 AS product_id,
    CONCAT('SESS', FLOOR(RAND() * 100000)) AS session_id,
    CONCAT('/product/', FLOOR(RAND() * 1000)) AS page_url,
    CONCAT('https://search.com?q=', FLOOR(RAND() * 100)) AS referer,
    DATE_ADD(
            (SELECT date_key FROM ods_dim_date ORDER BY RAND() LIMIT 1), -- 基于维度表日期生成访问时间
            INTERVAL FLOOR(RAND() * 86400) SECOND
    ) AS visit_time,
    ELT(FLOOR(RAND() * 3) + 1, 'Mobile','Desktop','Tablet') AS device_type,
    ELT(FLOOR(RAND() * 5) + 1, 'Chrome','Safari','Firefox','Edge','Opera') AS browser,
    ELT(FLOOR(RAND() * 10) + 1, 'Beijing','Shanghai','Guangzhou','Shenzhen','Chengdu','Hangzhou','Wuhan','Nanjing','Xi\'an','Chongqing') AS city,
    (SELECT date_key FROM ods_dim_date ORDER BY RAND() LIMIT 1) AS date_key  -- 直接从维度表随机取日期
FROM numbers;

-- 8. 独立访客日志表 (ods_fact_uv)
INSERT INTO ods_fact_uv (
    ip_address, cookie_id, user_id, date_key,
    product_id, channel, device_type
)
SELECT
    CONCAT(FLOOR(RAND() * 255), '.', FLOOR(RAND() * 255), '.', FLOOR(RAND() * 255), '.', FLOOR(RAND() * 255)) AS ip_address,
    CONCAT('COOKIE', FLOOR(RAND() * 1000000)) AS cookie_id,
    FLOOR(RAND() * 1000) + 1 AS user_id,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 365) DAY) AS date_key,
    FLOOR(RAND() * 1000) + 1 AS product_id,
    ELT(FLOOR(RAND() * 4) + 1, 'Organic','Paid','Social','Direct') AS channel,
    ELT(FLOOR(RAND() * 3) + 1, 'Mobile','Desktop','Tablet') AS device_type
FROM numbers;

-- 9. 流量来源明细表 (ods_fact_pv_source)
INSERT INTO ods_fact_pv_source (
    product_id, date_key, source_type, pv_count, uv_count
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS product_id,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 365) DAY) AS date_key,
    ELT(FLOOR(RAND() * 4) + 1, '自然搜索','付费搜索','社交','站外') AS source_type,
    FLOOR(RAND() * 1000) + 100 AS pv_count,
    FLOOR(RAND() * 500) + 50 AS uv_count
FROM numbers;

-- 10. 加购事件表 (ods_fact_cart_add)
INSERT INTO ods_fact_cart_add (
    user_id, product_id, sku_id, session_id,
    add_time, quantity, date_key
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS user_id,
    FLOOR(RAND() * 1000) + 1 AS product_id,
    FLOOR(RAND() * 1000) + 1 AS sku_id,
    CONCAT('SESS', FLOOR(RAND() * 100000)) AS session_id,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND) AS add_time,
    FLOOR(RAND() * 5) + 1 AS quantity,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 365) DAY) AS date_key
FROM numbers;

-- 11. 订单主表 (ods_fact_order)
INSERT INTO ods_fact_order (
    user_id, order_time, total_amount,
    payment_status, order_status, date_key
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS user_id,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND) AS order_time,
    ROUND(50 + RAND() * 500, 2) AS total_amount,
    ELT(FLOOR(RAND() * 3) + 1, 'PENDING','PAID','FAILED') AS payment_status,
    ELT(FLOOR(RAND() * 4) + 1, 'NEW','CONFIRMED','CANCELLED','COMPLETED') AS order_status,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 365) DAY) AS date_key
FROM numbers;

-- 12. 订单明细表 (ods_fact_order_item)
INSERT INTO ods_fact_order_item (
    order_id, product_id, sku_id, quantity,
    unit_price, subtotal_amount
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS order_id,
    FLOOR(RAND() * 1000) + 1 AS product_id,
    FLOOR(RAND() * 1000) + 1 AS sku_id,
    FLOOR(RAND() * 5) + 1 AS quantity,
    ROUND(10 + RAND() * 100, 2) AS unit_price,
    ROUND((10 + RAND() * 100) * (FLOOR(RAND() * 5) + 1), 2) AS subtotal_amount
FROM numbers;

-- 13. 支付日志表 (ods_fact_payment)
INSERT INTO ods_fact_payment (
    order_id, payment_method, payment_time,
    payment_amount, transaction_id
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS order_id,
    ELT(FLOOR(RAND() * 4) + 1, 'Alipay','WeChat','CreditCard','BankTransfer') AS payment_method,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND) AS payment_time,
    ROUND(50 + RAND() * 500, 2) AS payment_amount,
    CONCAT('TRX', FLOOR(RAND() * 1000000000)) AS transaction_id
FROM numbers;

-- 14. 退货退款日志表 (ods_fact_return)
INSERT INTO ods_fact_return (
    order_id, product_id, sku_id, return_time,
    return_quantity, return_reason, refund_amount
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS order_id,
    FLOOR(RAND() * 1000) + 1 AS product_id,
    FLOOR(RAND() * 1000) + 1 AS sku_id,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND) AS return_time,
    FLOOR(RAND() * 3) + 1 AS return_quantity,
    ELT(FLOOR(RAND() * 5) + 1, 'Damaged','Wrong Item','Size Issue','Color Issue','Changed Mind') AS return_reason,
    ROUND(10 + RAND() * 200, 2) AS refund_amount
FROM numbers;

-- 15. 商品评价表 (ods_fact_product_review)
INSERT INTO ods_fact_product_review (
    user_id, product_id, sku_id, rating,
    review_text, review_time
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS user_id,
    FLOOR(RAND() * 1000) + 1 AS product_id,
    FLOOR(RAND() * 1000) + 1 AS sku_id,
    FLOOR(RAND() * 5) + 1 AS rating,
    CONCAT('Review content for product ', FLOOR(RAND() * 1000)) AS review_text,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND) AS review_time
FROM numbers;

-- 16. 内容互动日志表 (ods_fact_content_engagement)
INSERT INTO ods_fact_content_engagement (
    user_id, product_id, engagement_type,
    engagement_time, platform
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS user_id,
    FLOOR(RAND() * 1000) + 1 AS product_id,
    ELT(FLOOR(RAND() * 4) + 1, '点赞','收藏','分享','咨询') AS engagement_type,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND) AS engagement_time,
    ELT(FLOOR(RAND() * 3) + 1, 'PC','H5','App') AS platform
FROM numbers;

-- 17. 促销活动日志表 (ods_fact_promotion_event)
INSERT INTO ods_fact_promotion_event (
    activity_id, product_id, sku_id, event_type,
    participation_count, order_count, start_time,
    end_time, date_key
)
SELECT
    CONCAT('ACT', FLOOR(RAND() * 10000)) AS activity_id,
    FLOOR(RAND() * 1000) + 1 AS product_id,
    FLOOR(RAND() * 1000) + 1 AS sku_id,
    ELT(FLOOR(RAND() * 3) + 1, '秒杀','满减','优惠券') AS event_type,
    FLOOR(RAND() * 1000) AS participation_count,
    FLOOR(RAND() * 500) AS order_count,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 180) DAY) AS start_time,
    DATE_ADD(DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 180) DAY), INTERVAL FLOOR(RAND() * 7) DAY) AS end_time,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 180) DAY) AS date_key
FROM numbers;

-- 18. 用户注册日志表 (ods_fact_user_register)
INSERT INTO ods_fact_user_register (
    user_id, register_channel, register_time,
    is_first_order, date_key
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS user_id,
    ELT(FLOOR(RAND() * 4) + 1, 'Web','App','WeChat','Offline') AS register_channel,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND) AS register_time,
    ELT(FLOOR(RAND() * 2) + 1, 'Y','N') AS is_first_order,
    DATE(DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND)) AS date_key
FROM numbers;

-- 19. 推荐拉新日志表 (ods_fact_referral)
INSERT INTO ods_fact_referral (
    inviter_user_id, invitee_user_id, invite_time,
    first_order_time, date_key
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS inviter_user_id,
    FLOOR(RAND() * 1000) + 1 AS invitee_user_id,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND) AS invite_time,
    DATE_ADD(DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND), INTERVAL FLOOR(RAND() * 30) DAY) AS first_order_time,
    DATE(DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND)) AS date_key
FROM numbers;

-- 20. 客服工单表 (ods_fact_service_ticket)
INSERT INTO ods_fact_service_ticket (
    user_id, product_id, ticket_type, create_time,
    close_time, resolution, agent_id, status,
    date_key
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS user_id,
    FLOOR(RAND() * 1000) + 1 AS product_id,
    ELT(FLOOR(RAND() * 3) + 1, '咨询','投诉','建议') AS ticket_type,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND) AS create_time,
    DATE_ADD(DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND), INTERVAL FLOOR(RAND() * 48) HOUR) AS close_time,
    CONCAT('Resolution for ticket ', n) AS resolution,
    FLOOR(RAND() * 100) + 1 AS agent_id,
    ELT(FLOOR(RAND() * 3) + 1, 'OPEN','CLOSED','PENDING') AS status,
    DATE(DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND)) AS date_key
FROM numbers;

-- 21. 售后服务表 (ods_fact_after_sales)
INSERT INTO ods_fact_after_sales (
    order_id, product_id, case_type, case_time,
    resolution_time, resolution_result, date_key
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS order_id,
    FLOOR(RAND() * 1000) + 1 AS product_id,
    ELT(FLOOR(RAND() * 4) + 1, '投诉','维权','返修','换货') AS case_type,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND) AS case_time,
    DATE_ADD(DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND), INTERVAL FLOOR(RAND() * 72) HOUR) AS resolution_time,
    CONCAT('Resolved case ', n) AS resolution_result,
    DATE(DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 31536000) SECOND)) AS date_key
FROM numbers;

-- 22. 竞品表现表 (ods_fact_competitor_performance)
INSERT INTO ods_fact_competitor_performance (
    comp_product_id, date_key, pv_count, uv_count,
    order_count, sales_amount, rating_average,
    price_change_flag
)
SELECT
    FLOOR(RAND() * 1000) + 1 AS comp_product_id,
    DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND() * 365) DAY) AS date_key,
    FLOOR(RAND() * 10000) AS pv_count,
    FLOOR(RAND() * 5000) AS uv_count,
    FLOOR(RAND() * 1000) AS order_count,
    ROUND(1000 + RAND() * 10000, 2) AS sales_amount,
    ROUND(1 + RAND() * 4, 2) AS rating_average,
    ELT(FLOOR(RAND() * 2) + 1, 'Y','N') AS price_change_flag
FROM numbers;

-- 删除临时表
DROP TEMPORARY TABLE numbers;