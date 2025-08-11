USE gmall_work_01;

-- 1. 商品访问事实表
CREATE TABLE dwd_item_visit_di (
                                   visit_id BIGINT COMMENT '访问ID',
                                   user_id BIGINT COMMENT '用户ID',
                                   item_id BIGINT COMMENT '商品ID',
                                   date_key INT COMMENT '日期键',
                                   visit_time TIMESTAMP COMMENT '访问时间',
                                   dwell_seconds INT COMMENT '停留时长(秒)',
                                   terminal_code STRING COMMENT '终端代码',
                                   device_type STRING COMMENT '设备类型',
                                   category_id INT COMMENT '类目ID',
                                   price DECIMAL(10,2) COMMENT '商品价格',
                                   etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'DWD-商品访问事实表'
    STORED AS ORC;

-- 2. 支付事实表
CREATE TABLE dwd_payment_di (
                                payment_id BIGINT COMMENT '支付ID',
                                order_id BIGINT COMMENT '订单ID',
                                item_id BIGINT COMMENT '商品ID',
                                user_id BIGINT COMMENT '用户ID',
                                date_key INT COMMENT '日期键',
                                payment_time TIMESTAMP COMMENT '支付时间',
                                payment_amount DECIMAL(12,2) COMMENT '支付金额',
                                method_id STRING COMMENT '支付方式ID',
                                method_name STRING COMMENT '支付方式名称',
                                activity_id STRING COMMENT '活动ID',
                                activity_name STRING COMMENT '活动名称',
                                category_id INT COMMENT '类目ID',
                                etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'DWD-支付事实表'
    STORED AS ORC;

-- 3. 下单事实表
CREATE TABLE dwd_order_di (
                              order_id BIGINT COMMENT '订单ID',
                              item_id BIGINT COMMENT '商品ID',
                              user_id BIGINT COMMENT '用户ID',
                              date_key INT COMMENT '日期键',
                              order_time TIMESTAMP COMMENT '下单时间',
                              quantity INT COMMENT '件数',
                              order_amount DECIMAL(12,2) COMMENT '订单金额',
                              order_status STRING COMMENT '订单状态',
                              category_id INT COMMENT '类目ID',
                              price DECIMAL(10,2) COMMENT '商品价格',
                              etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'DWD-下单事实表'
    STORED AS ORC;

-- 4. 商品收藏事实表
CREATE TABLE dwd_item_fav_di (
                                 fav_id BIGINT COMMENT '收藏ID',
                                 user_id BIGINT COMMENT '用户ID',
                                 item_id BIGINT COMMENT '商品ID',
                                 date_key INT COMMENT '日期键',
                                 fav_time TIMESTAMP COMMENT '收藏时间',
                                 action_type STRING COMMENT '动作类型',
                                 category_id INT COMMENT '类目ID',
                                 etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'DWD-商品收藏事实表'
    STORED AS ORC;

-- 5. 购物车操作事实表
CREATE TABLE dwd_cart_action_di (
                                    cart_id BIGINT COMMENT '操作ID',
                                    user_id BIGINT COMMENT '用户ID',
                                    item_id BIGINT COMMENT '商品ID',
                                    date_key INT COMMENT '日期键',
                                    cart_time TIMESTAMP COMMENT '操作时间',
                                    quantity INT COMMENT '件数',
                                    action_type STRING COMMENT '动作类型',
                                    category_id INT COMMENT '类目ID',
                                    price DECIMAL(10,2) COMMENT '商品价格',
                                    etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'DWD-购物车操作事实表'
    STORED AS ORC;

-- 6. 商品微详情浏览事实表
CREATE TABLE dwd_item_micro_detail_di (
                                          micro_id BIGINT COMMENT '浏览ID',
                                          user_id BIGINT COMMENT '用户ID',
                                          item_id BIGINT COMMENT '商品ID',
                                          date_key INT COMMENT '日期键',
                                          start_time TIMESTAMP COMMENT '开始时间',
                                          duration_sec INT COMMENT '浏览时长(秒)',
                                          terminal_code STRING COMMENT '终端代码',
                                          device_type STRING COMMENT '设备类型',
                                          category_id INT COMMENT '类目ID',
                                          etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT 'DWD-商品微详情浏览事实表'
    STORED AS ORC;

-- 1. 商品访问事实表
INSERT INTO dwd_item_visit_di
SELECT
    v.visit_id,
    v.user_id,
    v.item_id,
    v.date_key,
    v.visit_time,
    v.dwell_seconds,
    v.terminal,
    t.device_type,
    i.category_id,
    i.price,
    CURRENT_TIMESTAMP()
FROM ods_event_item_visit v
         JOIN dim_item i ON v.item_id = i.item_id
         JOIN dim_terminal t ON v.terminal = t.terminal_code;

-- 2. 支付事实表
INSERT INTO dwd_payment_di
SELECT
    p.payment_id,
    p.order_id,
    p.item_id,
    p.user_id,
    p.date_key,
    p.payment_time,
    p.payment_amount,
    p.payment_method,
    m.method_name,
    CASE
        WHEN p.is_juhuasuan = 1 THEN 'JUHUASUAN'
        WHEN p.is_presale = 1 THEN CONCAT('PRESALE-', p.presale_stage)
        ELSE 'NORMAL'
        END AS activity_id,
    CASE
        WHEN p.is_juhuasuan = 1 THEN '聚划算'
        WHEN p.is_presale = 1 THEN CONCAT('预售活动-',
                                          CASE p.presale_stage
                                              WHEN 'DEPOSIT' THEN '定金阶段'
                                              WHEN 'FINAL' THEN '尾款阶段'
                                              ELSE '全款阶段'
                                              END)
        ELSE '普通商品活动'
        END AS activity_name,
    i.category_id,
    CURRENT_TIMESTAMP()
FROM ods_event_payment p
         JOIN dim_payment_method m ON p.payment_method = m.method_id
         JOIN dim_item i ON p.item_id = i.item_id;

-- 3. 下单事实表
INSERT INTO dwd_order_di
SELECT
    o.order_id,
    o.item_id,
    o.user_id,
    o.date_key,
    o.order_time,
    o.quantity,
    o.order_amount,
    o.order_status,
    i.category_id,
    i.price,
    CURRENT_TIMESTAMP()
FROM ods_event_order o
         JOIN dim_item i ON o.item_id = i.item_id;

-- 4. 商品收藏事实表
INSERT INTO dwd_item_fav_di
SELECT
    f.fav_id,
    f.user_id,
    f.item_id,
    f.date_key,
    f.fav_time,
    f.action_type,
    i.category_id,
    CURRENT_TIMESTAMP()
FROM ods_event_item_favorite f
         JOIN dim_item i ON f.item_id = i.item_id;

-- 5. 购物车操作事实表
INSERT INTO dwd_cart_action_di
SELECT
    c.cart_id,
    c.user_id,
    c.item_id,
    c.date_key,
    c.cart_time,
    c.quantity,
    c.action_type,
    i.category_id,
    i.price,
    CURRENT_TIMESTAMP()
FROM ods_event_item_cart c
         JOIN dim_item i ON c.item_id = i.item_id;

-- 6. 商品微详情浏览事实表
INSERT INTO dwd_item_micro_detail_di
SELECT
    m.micro_id,
    m.user_id,
    m.item_id,
    m.date_key,
    m.start_time,
    m.duration_sec,
    m.terminal,
    t.device_type,
    i.category_id,
    CURRENT_TIMESTAMP()
FROM ods_event_item_micro_detail m
         JOIN dim_item i ON m.item_id = i.item_id
         JOIN dim_terminal t ON m.terminal = t.terminal_code;