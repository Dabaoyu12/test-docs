USE gmall_work_01;

-- 1. 商品行为日汇总表
CREATE TABLE dws_item_action_dd (
    date_key INT NOT NULL COMMENT '日期键',
    item_id BIGINT NOT NULL COMMENT '商品ID',
    item_name VARCHAR(255) COMMENT '商品名称',
    category_id INT COMMENT '类目ID',
    category_name VARCHAR(100) COMMENT '类目名称',
    price DECIMAL(10,2) COMMENT '商品价格',

    -- 访问指标
    visit_count BIGINT DEFAULT 0 COMMENT '访问次数',
    visitor_count BIGINT DEFAULT 0 COMMENT '访客数',
    total_dwell_seconds BIGINT DEFAULT 0 COMMENT '总停留时长(秒)',
    bounce_count BIGINT DEFAULT 0 COMMENT '跳出次数',

    -- 互动指标
    fav_add_count BIGINT DEFAULT 0 COMMENT '新增收藏数',
    fav_remove_count BIGINT DEFAULT 0 COMMENT '取消收藏数',
    cart_add_count BIGINT DEFAULT 0 COMMENT '加购次数',
    cart_remove_count BIGINT DEFAULT 0 COMMENT '移出购物车次数',
    cart_add_quantity BIGINT DEFAULT 0 COMMENT '加购件数',

    -- 转化指标
    order_count BIGINT DEFAULT 0 COMMENT '下单次数',
    order_quantity BIGINT DEFAULT 0 COMMENT '下单件数',
    order_amount DECIMAL(18,2) DEFAULT 0 COMMENT '下单金额',
    order_user_count BIGINT DEFAULT 0 COMMENT '下单用户数',

    -- 支付指标
    payment_count BIGINT DEFAULT 0 COMMENT '支付次数',
    payment_quantity BIGINT DEFAULT 0 COMMENT '支付件数',
    payment_amount DECIMAL(18,2) DEFAULT 0 COMMENT '支付金额',
    payment_user_count BIGINT DEFAULT 0 COMMENT '支付用户数',

    -- 微详情指标
    micro_detail_count BIGINT DEFAULT 0 COMMENT '微详情浏览次数',
    micro_detail_user_count BIGINT DEFAULT 0 COMMENT '微详情浏览用户数',

    etl_time DATETIME NOT NULL COMMENT 'ETL时间',
    PRIMARY KEY (date_key, item_id),
    KEY idx_category (category_id),
    KEY idx_price (price)
) COMMENT 'DWS-商品行为日汇总表';

-- 2. 用户商品行为日汇总表
CREATE TABLE dws_user_item_action_dd (
    date_key INT NOT NULL COMMENT '日期键',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    item_id BIGINT NOT NULL COMMENT '商品ID',

    -- 行为标记
    is_visit TINYINT(1) DEFAULT 0 COMMENT '是否访问',
    is_fav_add TINYINT(1) DEFAULT 0 COMMENT '是否新增收藏',
    is_cart_add TINYINT(1) DEFAULT 0 COMMENT '是否加购',
    is_order TINYINT(1) DEFAULT 0 COMMENT '是否下单',
    is_payment TINYINT(1) DEFAULT 0 COMMENT '是否支付',
    is_micro_detail TINYINT(1) DEFAULT 0 COMMENT '是否浏览微详情',

    -- 数值指标
    visit_dwell_seconds INT DEFAULT 0 COMMENT '停留时长(秒)',
    cart_add_quantity INT DEFAULT 0 COMMENT '加购件数',
    order_quantity INT DEFAULT 0 COMMENT '下单件数',
    order_amount DECIMAL(12,2) DEFAULT 0 COMMENT '下单金额',
    payment_amount DECIMAL(12,2) DEFAULT 0 COMMENT '支付金额',

    etl_time DATETIME NOT NULL COMMENT 'ETL时间',
    PRIMARY KEY (date_key, user_id, item_id),
    KEY idx_user (user_id),
    KEY idx_item (item_id)
) COMMENT 'DWS-用户商品行为日汇总表';

-- 3. 类目日汇总表
CREATE TABLE dws_category_action_dd (
    date_key INT NOT NULL COMMENT '日期键',
    category_id INT NOT NULL COMMENT '类目ID',
    category_name VARCHAR(100) COMMENT '类目名称',
    level TINYINT COMMENT '类目层级',

    -- 核心指标
    visitor_count BIGINT DEFAULT 0 COMMENT '访客数',
    fav_user_count BIGINT DEFAULT 0 COMMENT '收藏用户数',
    cart_user_count BIGINT DEFAULT 0 COMMENT '加购用户数',
    order_user_count BIGINT DEFAULT 0 COMMENT '下单用户数',
    payment_user_count BIGINT DEFAULT 0 COMMENT '支付用户数',
    payment_amount DECIMAL(18,2) DEFAULT 0 COMMENT '支付金额',

    -- 商品分布
    visited_item_count BIGINT DEFAULT 0 COMMENT '有访问商品数',
    ordered_item_count BIGINT DEFAULT 0 COMMENT '有下单商品数',
    paid_item_count BIGINT DEFAULT 0 COMMENT '有支付商品数',

    etl_time DATETIME NOT NULL COMMENT 'ETL时间',
    PRIMARY KEY (date_key, category_id),
    KEY idx_level (level)
) COMMENT 'DWS-类目行为日汇总表';

-- 4. 活动效果日汇总表
CREATE TABLE dws_activity_effect_dd (
    date_key INT NOT NULL COMMENT '日期键',
    activity_id VARCHAR(20) NOT NULL COMMENT '活动ID',
    activity_name VARCHAR(100) COMMENT '活动名称',

    -- 参与指标
    item_count BIGINT DEFAULT 0 COMMENT '参与商品数',
    visitor_count BIGINT DEFAULT 0 COMMENT '访客数',

    -- 转化指标
    order_count BIGINT DEFAULT 0 COMMENT '下单数',
    order_amount DECIMAL(18,2) DEFAULT 0 COMMENT '下单金额',
    payment_count BIGINT DEFAULT 0 COMMENT '支付数',
    payment_amount DECIMAL(18,2) DEFAULT 0 COMMENT '支付金额',
    payment_user_count BIGINT DEFAULT 0 COMMENT '支付用户数',

    -- 退款指标
    refund_count BIGINT DEFAULT 0 COMMENT '退款次数',
    refund_amount DECIMAL(18,2) DEFAULT 0 COMMENT '退款金额',

    etl_time DATETIME NOT NULL COMMENT 'ETL时间',
    PRIMARY KEY (date_key, activity_id)
) COMMENT 'DWS-活动效果日汇总表';
    
-- 5. 用户行为周/月汇总表
CREATE TABLE dws_user_action_summary (
    period_type ENUM('WEEK','MONTH') NOT NULL COMMENT '周期类型',
    period_key VARCHAR(10) NOT NULL COMMENT '周期键(YYYYWW/YYYYMM)',
    user_id BIGINT NOT NULL COMMENT '用户ID',

    -- 行为指标
    visit_count BIGINT DEFAULT 0 COMMENT '访问次数',
    fav_add_count BIGINT DEFAULT 0 COMMENT '收藏次数',
    cart_add_count BIGINT DEFAULT 0 COMMENT '加购次数',
    order_count BIGINT DEFAULT 0 COMMENT '下单次数',
    payment_count BIGINT DEFAULT 0 COMMENT '支付次数',

    -- 金额指标
    total_order_amount DECIMAL(18,2) DEFAULT 0 COMMENT '累计下单金额',
    total_payment_amount DECIMAL(18,2) DEFAULT 0 COMMENT '累计支付金额',

    -- 新老买家标记
    is_new_buyer TINYINT(1) DEFAULT 0 COMMENT '是否新买家',

    etl_time DATETIME NOT NULL COMMENT 'ETL时间',
    PRIMARY KEY (period_type, period_key, user_id)
) COMMENT 'DWS-用户行为周期汇总表';



-- 1. 商品行为日汇总表
INSERT INTO dws_item_action_dd (
    date_key, item_id, item_name, category_id, category_name, price,
    visit_count, visitor_count, total_dwell_seconds, bounce_count,
    fav_add_count, fav_remove_count, cart_add_count, cart_remove_count, cart_add_quantity,
    order_count, order_quantity, order_amount, order_user_count,
    payment_count, payment_quantity, payment_amount, payment_user_count,
    micro_detail_count, micro_detail_user_count,
    etl_time
)
SELECT
    date_key,
    item_id,
    MAX(item_name) AS item_name,
    MAX(category_id) AS category_id,
    MAX(category_name) AS category_name,
    MAX(price) AS price,

    -- 访问指标
    COUNT(DISTINCT visit_id) AS visit_count,
    COUNT(DISTINCT user_id) AS visitor_count,
    SUM(dwell_seconds) AS total_dwell_seconds,
    SUM(CASE WHEN dwell_seconds < 3 THEN 1 ELSE 0 END) AS bounce_count,

    -- 收藏指标
    SUM(CASE WHEN fav_action_type = 'ADD' THEN 1 ELSE 0 END) AS fav_add_count,
    SUM(CASE WHEN fav_action_type = 'REMOVE' THEN 1 ELSE 0 END) AS fav_remove_count,

    -- 购物车指标
    SUM(CASE WHEN cart_action_type = 'ADD' THEN 1 ELSE 0 END) AS cart_add_count,
    SUM(CASE WHEN cart_action_type = 'REMOVE' THEN 1 ELSE 0 END) AS cart_remove_count,
    SUM(cart_quantity) AS cart_add_quantity,

    -- 订单指标
    COUNT(DISTINCT order_id) AS order_count,
    SUM(order_quantity) AS order_quantity,
    SUM(order_amount) AS order_amount,
    COUNT(DISTINCT order_user_id) AS order_user_count,

    -- 支付指标
    COUNT(DISTINCT payment_id) AS payment_count,
    SUM(payment_quantity) AS payment_quantity,
    SUM(payment_amount) AS payment_amount,
    COUNT(DISTINCT payment_user_id) AS payment_user_count,

    -- 微详情指标
    COUNT(DISTINCT micro_id) AS micro_detail_count,
    COUNT(DISTINCT micro_user_id) AS micro_detail_user_count,

    NOW()
FROM (
         -- 访问数据
         SELECT
             date_key, item_id,
             visit_id, user_id, dwell_seconds,
             NULL AS fav_action_type, NULL AS cart_action_type,
             0 AS cart_quantity,
             NULL AS order_id, NULL AS order_user_id, 0 AS order_quantity, 0 AS order_amount,
             NULL AS payment_id, NULL AS payment_user_id, 0 AS payment_quantity, 0 AS payment_amount,
             NULL AS micro_id, NULL AS micro_user_id
         FROM dwd_item_visit_di

         UNION ALL

         -- 收藏数据
         SELECT
             date_key, item_id,
             NULL AS visit_id, user_id, 0 AS dwell_seconds,
             action_type AS fav_action_type, NULL AS cart_action_type,
             0 AS cart_quantity,
             NULL AS order_id, NULL AS order_user_id, 0 AS order_quantity, 0 AS order_amount,
             NULL AS payment_id, NULL AS payment_user_id, 0 AS payment_quantity, 0 AS payment_amount,
             NULL AS micro_id, NULL AS micro_user_id
         FROM dwd_item_fav_di

         UNION ALL

         -- 购物车数据
         SELECT
             date_key, item_id,
             NULL AS visit_id, user_id, 0 AS dwell_seconds,
             NULL AS fav_action_type, action_type AS cart_action_type,
             quantity AS cart_quantity,
             NULL AS order_id, NULL AS order_user_id, 0 AS order_quantity, 0 AS order_amount,
             NULL AS payment_id, NULL AS payment_user_id, 0 AS payment_quantity, 0 AS payment_amount,
             NULL AS micro_id, NULL AS micro_user_id
         FROM dwd_cart_action_di

         UNION ALL

         -- 订单数据
         SELECT
             date_key, item_id,
             NULL AS visit_id, user_id AS order_user_id, 0 AS dwell_seconds,
             NULL AS fav_action_type, NULL AS cart_action_type,
             0 AS cart_quantity,
             order_id, user_id, quantity AS order_quantity, order_amount,
             NULL AS payment_id, NULL AS payment_user_id, 0 AS payment_quantity, 0 AS payment_amount,
             NULL AS micro_id, NULL AS micro_user_id
         FROM dwd_order_di

         UNION ALL

         -- 支付数据
         SELECT
             date_key, item_id,
             NULL AS visit_id, user_id AS payment_user_id, 0 AS dwell_seconds,
             NULL AS fav_action_type, NULL AS cart_action_type,
             0 AS cart_quantity,
             order_id, NULL AS order_user_id, 0 AS order_quantity, 0 AS order_amount,
             payment_id, user_id, quantity AS payment_quantity, payment_amount,
             NULL AS micro_id, NULL AS micro_user_id
         FROM dwd_payment_di

         UNION ALL

         -- 微详情数据
         SELECT
             date_key, item_id,
             NULL AS visit_id, user_id AS micro_user_id, duration_sec AS dwell_seconds,
             NULL AS fav_action_type, NULL AS cart_action_type,
             0 AS cart_quantity,
             NULL AS order_id, NULL AS order_user_id, 0 AS order_quantity, 0 AS order_amount,
             NULL AS payment_id, NULL AS payment_user_id, 0 AS payment_quantity, 0 AS payment_amount,
             micro_id, user_id
         FROM dwd_item_micro_detail_di
     ) combined
         JOIN dim_item i USING (item_id)
         JOIN dim_category c ON i.category_id = c.category_id
GROUP BY date_key, item_id;

-- 2. 用户商品行为日汇总表
INSERT INTO dws_user_item_action_dd (
    date_key, user_id, item_id,
    is_visit, is_fav_add, is_cart_add, is_order, is_payment, is_micro_detail,
    visit_dwell_seconds, cart_add_quantity, order_quantity, order_amount, payment_amount,
    etl_time
)
SELECT
    date_key,
    user_id,
    item_id,

    -- 行为标记
    MAX(CASE WHEN visit_id IS NOT NULL THEN 1 ELSE 0 END) AS is_visit,
    MAX(CASE WHEN action_type = 'ADD' THEN 1 ELSE 0 END) AS is_fav_add,
    MAX(CASE WHEN action_type = 'ADD' AND cart_id IS NOT NULL THEN 1 ELSE 0 END) AS is_cart_add,
    MAX(CASE WHEN order_id IS NOT NULL THEN 1 ELSE 0 END) AS is_order,
    MAX(CASE WHEN payment_id IS NOT NULL THEN 1 ELSE 0 END) AS is_payment,
    MAX(CASE WHEN micro_id IS NOT NULL THEN 1 ELSE 0 END) AS is_micro_detail,

    -- 数值指标
    COALESCE(SUM(dwell_seconds), 0) AS visit_dwell_seconds,
    COALESCE(SUM(CASE WHEN action_type = 'ADD' THEN quantity ELSE 0 END), 0) AS cart_add_quantity,
    COALESCE(SUM(order_quantity), 0) AS order_quantity,
    COALESCE(SUM(order_amount), 0) AS order_amount,
    COALESCE(SUM(payment_amount), 0) AS payment_amount,

    NOW()
FROM (
         -- 访问数据
         SELECT date_key, user_id, item_id, visit_id, dwell_seconds,
                NULL AS action_type, NULL AS quantity, NULL AS cart_id,
                NULL AS order_id, NULL AS order_quantity, NULL AS order_amount,
                NULL AS payment_id, NULL AS payment_amount, NULL AS micro_id
         FROM dwd_item_visit_di

         UNION ALL

         -- 收藏数据
         SELECT date_key, user_id, item_id, NULL AS visit_id, NULL AS dwell_seconds,
                action_type, NULL AS quantity, NULL AS cart_id,
                NULL AS order_id, NULL AS order_quantity, NULL AS order_amount,
                NULL AS payment_id, NULL AS payment_amount, NULL AS micro_id
         FROM dwd_item_fav_di

         UNION ALL

         -- 购物车数据
         SELECT date_key, user_id, item_id, NULL AS visit_id, NULL AS dwell_seconds,
                action_type, quantity, cart_id AS cart_id,
                NULL AS order_id, NULL AS order_quantity, NULL AS order_amount,
                NULL AS payment_id, NULL AS payment_amount, NULL AS micro_id
         FROM dwd_cart_action_di

         UNION ALL

         -- 订单数据
         SELECT date_key, user_id, item_id, NULL AS visit_id, NULL AS dwell_seconds,
                NULL AS action_type, NULL AS quantity, NULL AS cart_id,
                order_id, quantity AS order_quantity, order_amount,
                NULL AS payment_id, NULL AS payment_amount, NULL AS micro_id
         FROM dwd_order_di

         UNION ALL

         -- 支付数据
         SELECT date_key, user_id, item_id, NULL AS visit_id, NULL AS dwell_seconds,
                NULL AS action_type, NULL AS quantity, NULL AS cart_id,
                order_id, NULL AS order_quantity, NULL AS order_amount,
                payment_id, payment_amount, NULL AS micro_id
         FROM dwd_payment_di

         UNION ALL

         -- 微详情数据
         SELECT date_key, user_id, item_id, NULL AS visit_id, duration_sec AS dwell_seconds,
                NULL AS action_type, NULL AS quantity, NULL AS cart_id,
                NULL AS order_id, NULL AS order_quantity, NULL AS order_amount,
                NULL AS payment_id, NULL AS payment_amount, micro_id
         FROM dwd_item_micro_detail_di
     ) combined
GROUP BY date_key, user_id, item_id;

-- 3. 类目日汇总表
INSERT INTO dws_category_action_dd (
    date_key, category_id, category_name, level,
    visitor_count, fav_user_count, cart_user_count,
    order_user_count, payment_user_count, payment_amount,
    visited_item_count, ordered_item_count, paid_item_count,
    etl_time
)
SELECT
    date_key,
    c.category_id,
    c.category_name,
    c.level,

    -- 用户指标
    COUNT(DISTINCT CASE WHEN is_visit = 1 THEN user_id END) AS visitor_count,
    COUNT(DISTINCT CASE WHEN is_fav_add = 1 THEN user_id END) AS fav_user_count,
    COUNT(DISTINCT CASE WHEN is_cart_add = 1 THEN user_id END) AS cart_user_count,
    COUNT(DISTINCT CASE WHEN is_order = 1 THEN user_id END) AS order_user_count,
    COUNT(DISTINCT CASE WHEN is_payment = 1 THEN user_id END) AS payment_user_count,

    -- 金额指标
    SUM(payment_amount) AS payment_amount,

    -- 商品指标
    COUNT(DISTINCT CASE WHEN is_visit = 1 THEN item_id END) AS visited_item_count,
    COUNT(DISTINCT CASE WHEN is_order = 1 THEN item_id END) AS ordered_item_count,
    COUNT(DISTINCT CASE WHEN is_payment = 1 THEN item_id END) AS paid_item_count,

    NOW()
FROM dws_user_item_action_dd a
         JOIN dim_item i ON a.item_id = i.item_id
         JOIN dim_category c ON i.category_id = c.category_id
GROUP BY date_key, c.category_id;

-- 4. 活动效果日汇总表
INSERT INTO dws_activity_effect_dd (
    date_key, activity_id, activity_name,
    item_count, visitor_count,
    order_count, order_amount, payment_count, payment_amount, payment_user_count,
    refund_count, refund_amount,
    etl_time
)
SELECT
    p.date_key,
    a.activity_id,
    a.activity_name,

    -- 参与指标
    COUNT(DISTINCT p.item_id) AS item_count,
    COUNT(DISTINCT p.user_id) AS visitor_count,

    -- 转化指标
    COUNT(DISTINCT p.order_id) AS order_count,
    SUM(p.order_amount) AS order_amount,
    COUNT(DISTINCT p.payment_id) AS payment_count,
    SUM(p.payment_amount) AS payment_amount,
    COUNT(DISTINCT p.user_id) AS payment_user_count,

    -- 退款指标
    COUNT(DISTINCT r.refund_id) AS refund_count,
    SUM(r.refund_amount) AS refund_amount,

    NOW()
FROM dwd_payment_di p
         LEFT JOIN ods_event_refund r ON p.payment_id = r.payment_id
         JOIN dim_activity a ON p.activity_id = a.activity_id
GROUP BY p.date_key, a.activity_id;

-- 5. 用户行为周/月汇总表
INSERT INTO dws_user_action_summary (
    period_type, period_key, user_id,
    visit_count, fav_add_count, cart_add_count, order_count, payment_count,
    total_order_amount, total_payment_amount,
    is_new_buyer,
    etl_time
)
SELECT
    period_type,
    period_key,
    user_id,

    -- 行为指标
    COUNT(DISTINCT CASE WHEN is_visit = 1 THEN date_key END) AS visit_count,
    SUM(is_fav_add) AS fav_add_count,
    SUM(is_cart_add) AS cart_add_count,
    SUM(is_order) AS order_count,
    SUM(is_payment) AS payment_count,

    -- 金额指标
    SUM(order_amount) AS total_order_amount,
    SUM(payment_amount) AS total_payment_amount,

    -- 新老买家标记 (基于用户维度表)
    CASE
        WHEN u.last_payment_date < DATE_SUB(d.date, INTERVAL 365 DAY)
            OR u.last_payment_date IS NULL THEN 1
        ELSE 0
        END AS is_new_buyer,

    NOW()
FROM (
         SELECT
             'WEEK' AS period_type,
             DATE_FORMAT(d.date, '%x%v') AS period_key,
             a.*
         FROM dws_user_item_action_dd a
                  JOIN dim_date d ON a.date_key = d.date_key

         UNION ALL

         SELECT
             'MONTH' AS period_type,
             DATE_FORMAT(d.date, '%Y%m') AS period_key,
             a.*
         FROM dws_user_item_action_dd a
                  JOIN dim_date d ON a.date_key = d.date_key
     ) period_data
         JOIN dim_user u USING (user_id)
         JOIN dim_date d ON period_data.date_key = d.date_key
GROUP BY period_type, period_key, user_id;
