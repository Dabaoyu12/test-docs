USE gmall_work_01;

-- 1. 商品行为日汇总表
CREATE TABLE dws_item_action_dd (
    date_key INT COMMENT '日期键',
    item_id BIGINT COMMENT '商品ID',
    item_name STRING COMMENT '商品名称',
    category_id INT COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    price DECIMAL(10,2) COMMENT '商品价格',
    -- 访问指标
    visit_count BIGINT COMMENT '访问次数',
    visitor_count BIGINT COMMENT '访客数',
    total_dwell_seconds BIGINT COMMENT '总停留时长(秒)',
    bounce_count BIGINT COMMENT '跳出次数',
    -- 互动指标
    fav_add_count BIGINT COMMENT '新增收藏数',
    fav_remove_count BIGINT COMMENT '取消收藏数',
    cart_add_count BIGINT COMMENT '加购次数',
    cart_remove_count BIGINT COMMENT '移出购物车次数',
    cart_add_quantity BIGINT COMMENT '加购件数',
    -- 转化指标
    order_count BIGINT COMMENT '下单次数',
    order_quantity BIGINT COMMENT '下单件数',
    order_amount DECIMAL(18,2) COMMENT '下单金额',
    order_user_count BIGINT COMMENT '下单用户数',
    -- 支付指标
    payment_count BIGINT COMMENT '支付次数',
    payment_quantity BIGINT COMMENT '支付件数',
    payment_amount DECIMAL(18,2) COMMENT '支付金额',
    payment_user_count BIGINT COMMENT '支付用户数',
    -- 微详情指标
    micro_detail_count BIGINT COMMENT '微详情浏览次数',
    micro_detail_user_count BIGINT COMMENT '微详情浏览用户数',
    etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'DWS-商品行为日汇总表'
    STORED AS ORC;

-- 2. 用户商品行为日汇总表
CREATE TABLE dws_user_item_action_dd (
     date_key INT COMMENT '日期键',
     user_id BIGINT COMMENT '用户ID',
     item_id BIGINT COMMENT '商品ID',
    -- 行为标记
     is_visit TINYINT COMMENT '是否访问',
     is_fav_add TINYINT COMMENT '是否新增收藏',
     is_cart_add TINYINT COMMENT '是否加购',
     is_order TINYINT COMMENT '是否下单',
     is_payment TINYINT COMMENT '是否支付',
     is_micro_detail TINYINT COMMENT '是否浏览微详情',
    -- 数值指标
     visit_dwell_seconds INT COMMENT '停留时长(秒)',
     cart_add_quantity INT COMMENT '加购件数',
     order_quantity INT COMMENT '下单件数',
     order_amount DECIMAL(12,2) COMMENT '下单金额',
     payment_amount DECIMAL(12,2) COMMENT '支付金额',
     etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'DWS-用户商品行为日汇总表'
    STORED AS ORC;
    
-- 3. 类目日汇总表
CREATE TABLE dws_category_action_dd (
    date_key INT COMMENT '日期键',
    category_id INT COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    level TINYINT COMMENT '类目层级',
    -- 核心指标
    visitor_count BIGINT COMMENT '访客数',
    fav_user_count BIGINT COMMENT '收藏用户数',
    cart_user_count BIGINT COMMENT '加购用户数',
    order_user_count BIGINT COMMENT '下单用户数',
    payment_user_count BIGINT COMMENT '支付用户数',
    payment_amount DECIMAL(18,2) COMMENT '支付金额',
    -- 商品分布
    visited_item_count BIGINT COMMENT '有访问商品数',
    ordered_item_count BIGINT COMMENT '有下单商品数',
    paid_item_count BIGINT COMMENT '有支付商品数',
    etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'DWS-类目行为日汇总表'
    STORED AS ORC;

-- 4. 活动效果日汇总表
CREATE TABLE dws_activity_effect_dd (
    date_key INT COMMENT '日期键',
    activity_id STRING COMMENT '活动ID',
    activity_name STRING COMMENT '活动名称',
    -- 参与指标
    item_count BIGINT COMMENT '参与商品数',
    visitor_count BIGINT COMMENT '访客数',
    -- 转化指标
    order_count BIGINT COMMENT '下单数',
    total_order_amount DECIMAL(18,2) COMMENT '累计下单金额',
    paid_order_amount DECIMAL(18,2) COMMENT '已支付订单金额',
    payment_count BIGINT COMMENT '支付数',
    payment_amount DECIMAL(18,2) COMMENT '支付金额',
    payment_user_count BIGINT COMMENT '支付用户数',
    -- 退款指标
    refund_count BIGINT COMMENT '退款次数',
    refund_amount DECIMAL(18,2) COMMENT '退款金额',
    etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'DWS-活动效果日汇总表'
    STORED AS ORC;

-- 5. 用户行为周/月汇总表
DROP TABLE IF EXISTS dws_user_action_summary;
CREATE TABLE dws_user_action_summary (
     period_type STRING COMMENT '周期类型',
     period_key STRING COMMENT '周期键(YYYYWW/YYYYMM)',
     user_id BIGINT COMMENT '用户ID',
    -- 行为指标
     visit_count BIGINT COMMENT '访问次数',
     fav_add_count BIGINT COMMENT '收藏次数',
     cart_add_count BIGINT COMMENT '加购次数',
     order_count BIGINT COMMENT '下单次数',
     payment_count BIGINT COMMENT '支付次数',
    -- 金额指标
     total_order_amount DECIMAL(18,2) COMMENT '累计下单金额',
     total_payment_amount DECIMAL(18,2) COMMENT '累计支付金额',
    -- 新老买家标记
     is_new_buyer TINYINT COMMENT '是否新买家',
     etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'DWS-用户行为周期汇总表'
    STORED AS ORC;

-- 删除原表
DROP TABLE IF EXISTS dwd_payment_di;

-- 重建支付事实表（添加 quantity 字段）
CREATE TABLE dwd_payment_di (
                                payment_id BIGINT COMMENT '支付ID',
                                order_id BIGINT COMMENT '订单ID',
                                item_id BIGINT COMMENT '商品ID',
                                user_id BIGINT COMMENT '用户ID',
                                date_key INT COMMENT '日期键',
                                payment_time TIMESTAMP COMMENT '支付时间',
                                payment_amount DECIMAL(12,2) COMMENT '支付金额',
                                quantity INT COMMENT '支付件数',
                                method_id STRING COMMENT '支付方式ID',
                                method_name STRING COMMENT '支付方式名称',
                                activity_id STRING COMMENT '活动ID',
                                activity_name STRING COMMENT '活动名称',
                                category_id INT COMMENT '类目ID',
                                etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'DWD-支付事实表'
    STORED AS ORC;

-- 重新填充支付事实表数据
INSERT INTO dwd_payment_di
SELECT
    p.payment_id,
    p.order_id,
    p.item_id,
    p.user_id,
    p.date_key,
    p.payment_time,
    p.payment_amount,
    COALESCE(o.quantity, 1) AS quantity,
    p.payment_method AS method_id,
    m.method_name,
    a.activity_id,
    a.activity_name,
    i.category_id,
    CURRENT_TIMESTAMP()
FROM ods_event_payment p
         JOIN dim_payment_method m ON p.payment_method = m.method_id
         JOIN dim_item i ON p.item_id = i.item_id
         LEFT JOIN ods_event_order o
                   ON p.order_id = o.order_id AND p.item_id = o.item_id
         LEFT JOIN dim_activity a ON
    CASE
        WHEN p.is_juhuasuan = 1 THEN 'JUHUASUAN'
        WHEN p.is_presale = 1 THEN CONCAT('PRESALE-', p.presale_stage)
        ELSE 'NORMAL'
        END = a.activity_id;

-- 创建中间表替代临时表
CREATE TABLE temp_combined_data (
    date_key INT,
    item_id BIGINT,
    visit_id BIGINT,
    user_id BIGINT,
    dwell_seconds INT,
    fav_action_type STRING,
    cart_action_type STRING,
    cart_quantity INT,
    order_id BIGINT,
    order_user_id BIGINT,
    order_quantity INT,
    order_amount DECIMAL(12,2),
    payment_id BIGINT,
    payment_user_id BIGINT,
    payment_quantity INT,
    payment_amount DECIMAL(12,2),
    micro_id BIGINT,
    micro_user_id BIGINT
) STORED AS ORC;

-- 填充中间表
INSERT INTO temp_combined_data
SELECT
    date_key, item_id,
    visit_id, user_id, dwell_seconds,
    fav_action_type, cart_action_type,
    cart_quantity,
    order_id, order_user_id, order_quantity, order_amount,
    payment_id, payment_user_id, payment_quantity, payment_amount,
    micro_id, micro_user_id
FROM (
         -- 访问数据
         SELECT
             date_key, item_id,
             visit_id, user_id, dwell_seconds,
             NULL AS fav_action_type, NULL AS cart_action_type,
             0 AS cart_quantity,
             NULL AS order_id, NULL AS order_user_id, 0 AS order_quantity, 0.0 AS order_amount,
             NULL AS payment_id, NULL AS payment_user_id, 0 AS payment_quantity, 0.0 AS payment_amount,
             NULL AS micro_id, NULL AS micro_user_id
         FROM dwd_item_visit_di

         UNION ALL

         -- 收藏数据
         SELECT
             date_key, item_id,
             NULL, user_id, 0,
             action_type, NULL,
             0,
             NULL, NULL, 0, 0.0,
             NULL, NULL, 0, 0.0,
             NULL, NULL
         FROM dwd_item_fav_di

         UNION ALL

         -- 购物车数据
         SELECT
             date_key, item_id,
             NULL, user_id, 0,
             NULL, action_type,
             quantity,
             NULL, NULL, 0, 0.0,
             NULL, NULL, 0, 0.0,
             NULL, NULL
         FROM dwd_cart_action_di

         UNION ALL

         -- 订单数据
         SELECT
             date_key, item_id,
             NULL, user_id AS order_user_id, 0,
             NULL, NULL,
             0,
             order_id, user_id, quantity, order_amount,
             NULL, NULL, 0, 0.0,
             NULL, NULL
         FROM dwd_order_di

         UNION ALL

         -- 支付数据
         SELECT
             date_key, item_id,
             NULL, user_id AS payment_user_id, 0,
             NULL, NULL,
             0,
             order_id, NULL, 0, 0.0,
             payment_id, user_id, quantity, payment_amount,
             NULL, NULL
         FROM dwd_payment_di

         UNION ALL

         -- 微详情数据
         SELECT
             date_key, item_id,
             NULL, user_id AS micro_user_id, duration_sec,
             NULL, NULL,
             0,
             NULL, NULL, 0, 0.0,
             NULL, NULL, 0, 0.0,
             micro_id, user_id
         FROM dwd_item_micro_detail_di
     ) combined_data;

-- 插入DWS商品行为日汇总表
INSERT INTO dws_item_action_dd
SELECT
    t.date_key,
    t.item_id,
    MAX(i.item_name) AS item_name,
    MAX(i.category_id) AS category_id,
    MAX(c.category_name) AS category_name,
    MAX(i.price) AS price,
    COUNT(DISTINCT t.visit_id) AS visit_count,
    COUNT(DISTINCT t.user_id) AS visitor_count,
    SUM(t.dwell_seconds) AS total_dwell_seconds,
    SUM(CASE WHEN t.dwell_seconds < 3 THEN 1 ELSE 0 END) AS bounce_count,
    SUM(CASE WHEN t.fav_action_type = 'ADD' THEN 1 ELSE 0 END) AS fav_add_count,
    SUM(CASE WHEN t.fav_action_type = 'REMOVE' THEN 1 ELSE 0 END) AS fav_remove_count,
    SUM(CASE WHEN t.cart_action_type = 'ADD' THEN 1 ELSE 0 END) AS cart_add_count,
    SUM(CASE WHEN t.cart_action_type = 'REMOVE' THEN 1 ELSE 0 END) AS cart_remove_count,
    SUM(t.cart_quantity) AS cart_add_quantity,
    COUNT(DISTINCT t.order_id) AS order_count,
    SUM(t.order_quantity) AS order_quantity,
    SUM(t.order_amount) AS order_amount,
    COUNT(DISTINCT t.order_user_id) AS order_user_count,
    COUNT(DISTINCT t.payment_id) AS payment_count,
    SUM(t.payment_quantity) AS payment_quantity,
    SUM(t.payment_amount) AS payment_amount,
    COUNT(DISTINCT t.payment_user_id) AS payment_user_count,
    COUNT(DISTINCT t.micro_id) AS micro_detail_count,
    COUNT(DISTINCT t.micro_user_id) AS micro_detail_user_count,
    CURRENT_TIMESTAMP()
FROM temp_combined_data t
         JOIN dim_item i ON t.item_id = i.item_id
         LEFT JOIN dim_category c ON i.category_id = c.category_id
GROUP BY t.date_key, t.item_id;

-- 清理中间表
DROP TABLE temp_combined_data;

-- 插入用户商品行为日汇总表
INSERT INTO dws_user_item_action_dd
SELECT
    date_key,
    user_id,
    item_id,
    MAX(CASE WHEN visit_id IS NOT NULL THEN 1 ELSE 0 END) AS is_visit,
    MAX(CASE WHEN action_type = 'ADD' THEN 1 ELSE 0 END) AS is_fav_add,
    MAX(CASE WHEN action_type = 'ADD' AND cart_id IS NOT NULL THEN 1 ELSE 0 END) AS is_cart_add,
    MAX(CASE WHEN order_id IS NOT NULL THEN 1 ELSE 0 END) AS is_order,
    MAX(CASE WHEN payment_id IS NOT NULL THEN 1 ELSE 0 END) AS is_payment,
    MAX(CASE WHEN micro_id IS NOT NULL THEN 1 ELSE 0 END) AS is_micro_detail,
    COALESCE(SUM(dwell_seconds), 0) AS visit_dwell_seconds,
    COALESCE(SUM(CASE WHEN action_type = 'ADD' THEN quantity ELSE 0 END), 0) AS cart_add_quantity,
    COALESCE(SUM(order_quantity), 0) AS order_quantity,
    COALESCE(SUM(order_amount), 0) AS order_amount,
    COALESCE(SUM(payment_amount), 0) AS payment_amount,
    CURRENT_TIMESTAMP()
FROM (
         SELECT date_key, user_id, item_id, visit_id, dwell_seconds,
                NULL AS action_type, NULL AS quantity, NULL AS cart_id,
                NULL AS order_id, NULL AS order_quantity, NULL AS order_amount,
                NULL AS payment_id, NULL AS payment_amount, NULL AS micro_id
         FROM dwd_item_visit_di

         UNION ALL

         SELECT date_key, user_id, item_id, NULL, NULL,
                action_type, NULL, NULL,
                NULL, NULL, NULL,
                NULL, NULL, NULL
         FROM dwd_item_fav_di

         UNION ALL

         SELECT date_key, user_id, item_id, NULL, NULL,
                action_type, quantity, cart_id,
                NULL, NULL, NULL,
                NULL, NULL, NULL
         FROM dwd_cart_action_di

         UNION ALL

         SELECT date_key, user_id, item_id, NULL, NULL,
                NULL, NULL, NULL,
                order_id, quantity, order_amount,
                NULL, NULL, NULL
         FROM dwd_order_di

         UNION ALL

         SELECT date_key, user_id, item_id, NULL, NULL,
                NULL, NULL, NULL,
                order_id, NULL, NULL,
                payment_id, payment_amount, NULL
         FROM dwd_payment_di

         UNION ALL

         SELECT date_key, user_id, item_id, NULL, duration_sec,
                NULL, NULL, NULL,
                NULL, NULL, NULL,
                NULL, NULL, micro_id
         FROM dwd_item_micro_detail_di
     ) combined
GROUP BY date_key, user_id, item_id;

-- Hive不支持UPDATE，改为重新计算并覆盖dim_item表
-- 原UPDATE语句已移除，确保在ETL过程中正确处理数据

-- 插入类目日汇总表
INSERT INTO dws_category_action_dd
SELECT
    a.date_key,
    c.category_id,
    c.category_name,
    c.level,
    COUNT(DISTINCT CASE WHEN a.is_visit = 1 THEN a.user_id END) AS visitor_count,
    COUNT(DISTINCT CASE WHEN a.is_fav_add = 1 THEN a.user_id END) AS fav_user_count,
    COUNT(DISTINCT CASE WHEN a.is_cart_add = 1 THEN a.user_id END) AS cart_user_count,
    COUNT(DISTINCT CASE WHEN a.is_order = 1 THEN a.user_id END) AS order_user_count,
    COUNT(DISTINCT CASE WHEN a.is_payment = 1 THEN a.user_id END) AS payment_user_count,
    SUM(a.payment_amount) AS payment_amount,
    COUNT(DISTINCT CASE WHEN a.is_visit = 1 THEN a.item_id END) AS visited_item_count,
    COUNT(DISTINCT CASE WHEN a.is_order = 1 THEN a.item_id END) AS ordered_item_count,
    COUNT(DISTINCT CASE WHEN a.is_payment = 1 THEN a.item_id END) AS paid_item_count,
    CURRENT_TIMESTAMP()
FROM dws_user_item_action_dd a
         LEFT JOIN dim_item i ON a.item_id = i.item_id
         LEFT JOIN dim_category c ON i.category_id = c.category_id
WHERE c.category_id IS NOT NULL
GROUP BY a.date_key, c.category_id, c.category_name, c.level;

-- 插入活动效果日汇总表
INSERT INTO dws_activity_effect_dd
SELECT
    p.date_key,
    a.activity_id,
    a.activity_name,
    COUNT(DISTINCT p.item_id) AS item_count,
    COUNT(DISTINCT p.user_id) AS visitor_count,
    COUNT(DISTINCT o.order_id) AS order_count,
    COALESCE(SUM(o.order_amount), 0) AS total_order_amount,
    COALESCE(SUM(CASE WHEN p.payment_id IS NOT NULL THEN o.order_amount ELSE 0 END), 0) AS paid_order_amount,
    COUNT(DISTINCT p.payment_id) AS payment_count,
    COALESCE(SUM(p.payment_amount), 0) AS payment_amount,
    COUNT(DISTINCT p.user_id) AS payment_user_count,
    COUNT(DISTINCT r.refund_id) AS refund_count,
    COALESCE(SUM(r.refund_amount), 0) AS refund_amount,
    CURRENT_TIMESTAMP()
FROM dim_activity a
         LEFT JOIN dwd_payment_di p ON a.activity_id = p.activity_id
         LEFT JOIN dwd_order_di o ON p.order_id = o.order_id AND p.item_id = o.item_id
         LEFT JOIN ods_event_refund r ON p.payment_id = r.payment_id
GROUP BY p.date_key, a.activity_id, a.activity_name;

-- 插入用户行为周/月汇总表
INSERT INTO dws_user_action_summary
SELECT
    pd.period_type,
    pd.period_key,
    pd.user_id,
    pd.visit_count,
    pd.fav_add_count,
    pd.cart_add_count,
    pd.order_count,
    pd.payment_count,
    pd.total_order_amount,
    pd.total_payment_amount,
    CASE
        WHEN u.last_payment_date < DATE_SUB(pd.period_start_date, 365)
            OR u.last_payment_date IS NULL THEN 1
        ELSE 0
        END AS is_new_buyer,
    CURRENT_TIMESTAMP()
FROM (
         SELECT
             'WEEK' AS period_type,
             DATE_FORMAT(d.date_value, 'yyyyww') AS period_key,
             MIN(d.date_value) AS period_start_date,
             a.user_id,
             COUNT(DISTINCT CASE WHEN a.is_visit = 1 THEN a.date_key END) AS visit_count,
             SUM(a.is_fav_add) AS fav_add_count,
             SUM(a.is_cart_add) AS cart_add_count,
             SUM(a.is_order) AS order_count,
             SUM(a.is_payment) AS payment_count,
             SUM(a.order_amount) AS total_order_amount,
             SUM(a.payment_amount) AS total_payment_amount
         FROM dws_user_item_action_dd a
                  JOIN dim_date d ON a.date_key = d.date_key
         GROUP BY DATE_FORMAT(d.date_value, 'yyyyww'), a.user_id

         UNION ALL

         SELECT
             'MONTH' AS period_type,
             DATE_FORMAT(d.date_value, 'yyyyMM') AS period_key,
             MIN(d.date_value) AS period_start_date,
             a.user_id,
             COUNT(DISTINCT CASE WHEN a.is_visit = 1 THEN a.date_key END) AS visit_count,
             SUM(a.is_fav_add) AS fav_add_count,
             SUM(a.is_cart_add) AS cart_add_count,
             SUM(a.is_order) AS order_count,
             SUM(a.is_payment) AS payment_count,
             SUM(a.order_amount) AS total_order_amount,
             SUM(a.payment_amount) AS total_payment_amount
         FROM dws_user_item_action_dd a
                  JOIN dim_date d ON a.date_key = d.date_key
         GROUP BY DATE_FORMAT(d.date_value, 'yyyyMM'), a.user_id
     ) pd
         JOIN dim_user u ON pd.user_id = u.user_id;