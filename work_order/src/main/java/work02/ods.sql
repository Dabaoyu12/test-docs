USE gmall_work_02;
-- 1. 商品基础信息表
DROP TABLE IF EXISTS ods_product_info;
CREATE TABLE ods_product_info (
    product_id           BIGINT       NOT NULL COMMENT '商品ID',
    product_name         VARCHAR(200) NOT NULL COMMENT '商品名称',
    brand_id             BIGINT       NOT NULL COMMENT '品牌ID',
    vendor_id            BIGINT       NOT NULL COMMENT '供应商ID',
    first_cat_id         INT          NOT NULL COMMENT '一级类目',
    second_cat_id        INT          NOT NULL COMMENT '二级类目',
    third_cat_id         INT          NOT NULL COMMENT '三级类目',
    create_time          DATETIME     NOT NULL COMMENT '商品首次上架时间',
    PRIMARY KEY (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 商品基础信息原始表';

-- 2. SKU 信息表
DROP TABLE IF EXISTS ods_sku_info;
CREATE TABLE ods_sku_info (
    sku_id               BIGINT       NOT NULL COMMENT 'SKU ID',
    product_id           BIGINT       NOT NULL COMMENT '关联商品ID',
    sku_name             VARCHAR(200) NOT NULL COMMENT 'SKU 名称/属性',
    create_time          DATETIME     NOT NULL COMMENT 'SKU 创建时间',
    PRIMARY KEY (sku_id),
    INDEX idx_product (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: SKU 维度表';

-- 3. 订单头表
DROP TABLE IF EXISTS ods_order_header;
CREATE TABLE ods_order_header (
    order_id             BIGINT       NOT NULL COMMENT '订单ID',
    user_id              BIGINT       NOT NULL COMMENT '买家ID',
    order_time           DATETIME     NOT NULL COMMENT '下单时间',
    pay_time             DATETIME     NULL COMMENT '支付时间',
    order_status         VARCHAR(20)  NOT NULL COMMENT '订单状态',
    store_id             BIGINT       NOT NULL COMMENT '店铺ID',
    total_amount         DECIMAL(10,2) NOT NULL COMMENT '订单总金额',
    total_quantity       INT          NOT NULL COMMENT '订单总件数',
    coupon_id            BIGINT       NULL COMMENT '使用优惠券ID',
    PRIMARY KEY (order_id),
    INDEX idx_user (user_id),
    INDEX idx_time (order_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 订单头原始表';

-- 4. 订单明细表
DROP TABLE IF EXISTS ods_order_item;
CREATE TABLE ods_order_item (
    order_id             BIGINT       NOT NULL COMMENT '订单ID',
    item_id              BIGINT       NOT NULL COMMENT '明细ID',
    product_id           BIGINT       NOT NULL COMMENT '商品ID',
    sku_id               BIGINT       NOT NULL COMMENT 'SKU ID',
    quantity             INT          NOT NULL COMMENT '购买数量',
    price                DECIMAL(10,2) NOT NULL COMMENT '单价',
    amount               DECIMAL(10,2) NOT NULL COMMENT '小计金额',
    PRIMARY KEY (item_id),
    INDEX idx_order (order_id),
    INDEX idx_product (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 订单明细原始表';

-- 5. 商品访问日志
DROP TABLE IF EXISTS ods_product_visit_log;
CREATE TABLE ods_product_visit_log (
        log_id               BIGINT       NOT NULL AUTO_INCREMENT COMMENT '日志ID',
        user_id              BIGINT       NOT NULL COMMENT '访问用户ID',
        session_id           VARCHAR(64)  NOT NULL COMMENT '会话ID',
        product_id           BIGINT       NOT NULL COMMENT '商品ID',
        event_time           DATETIME     NOT NULL COMMENT '访问时间',
        entry_page           VARCHAR(100) NOT NULL COMMENT '入口页面',
        referrer_page        VARCHAR(100) NULL COMMENT '来源页面',
        device_type          VARCHAR(20)  NOT NULL COMMENT '设备类型',
        region               VARCHAR(50)  NOT NULL COMMENT '访问地区',
        PRIMARY KEY (log_id),
        INDEX idx_product_time (product_id, event_time),
        INDEX idx_user_time (user_id, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 商品详情页访问日志';

-- 6. 加购 / 移出购物车日志
DROP TABLE IF EXISTS ods_cart_action_log;
CREATE TABLE ods_cart_action_log (
      log_id               BIGINT       NOT NULL AUTO_INCREMENT COMMENT '日志ID',
      user_id              BIGINT       NOT NULL COMMENT '用户ID',
      session_id           VARCHAR(64)  NOT NULL COMMENT '会话ID',
      product_id           BIGINT       NOT NULL COMMENT '商品ID',
      sku_id               BIGINT       NOT NULL COMMENT 'SKU ID',
      action_time          DATETIME     NOT NULL COMMENT '操作时间',
      action_type          ENUM('ADD','REMOVE') NOT NULL COMMENT '加购/移除',
      quantity             INT          NOT NULL COMMENT '操作件数',
      PRIMARY KEY (log_id),
      INDEX idx_prod_time (product_id, action_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 购物车操作日志';

-- 7. 搜索日志
DROP TABLE IF EXISTS ods_search_log;
CREATE TABLE ods_search_log (
    log_id               BIGINT       NOT NULL AUTO_INCREMENT COMMENT '日志ID',
    user_id              BIGINT       NOT NULL COMMENT '用户ID',
    session_id           VARCHAR(64)  NOT NULL COMMENT '会话ID',
    search_keyword       VARCHAR(200) NOT NULL COMMENT '搜索词',
    result_count         INT          NOT NULL COMMENT '命中结果数',
    clicked_product_id   BIGINT       NULL COMMENT '点击的商品ID',
    search_time          DATETIME     NOT NULL COMMENT '搜索时间',
    PRIMARY KEY (log_id),
    INDEX idx_keyword (search_keyword),
    INDEX idx_user_time (user_id, search_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 搜索行为日志';

-- 8. 流量来源日志
DROP TABLE IF EXISTS ods_traffic_source_log;
CREATE TABLE ods_traffic_source_log (
         log_id               BIGINT       NOT NULL AUTO_INCREMENT COMMENT '日志ID',
         session_id           VARCHAR(64)  NOT NULL COMMENT '会话ID',
         user_id              BIGINT       NOT NULL COMMENT '用户ID',
         source_type          VARCHAR(50)  NOT NULL COMMENT '流量来源渠道',
         visit_time           DATETIME     NOT NULL COMMENT '访问时间',
         PRIMARY KEY (log_id),
         INDEX idx_source_time (source_type, visit_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 流量来源埋点日志';

-- 9. 商品运营事件日志
DROP TABLE IF EXISTS ods_product_event_log;
CREATE TABLE ods_product_event_log (
        event_id             BIGINT       NOT NULL AUTO_INCREMENT COMMENT '事件ID',
        product_id           BIGINT       NOT NULL COMMENT '商品ID',
        user_id              BIGINT       NOT NULL COMMENT '操作者ID',
        event_type           VARCHAR(100) NOT NULL COMMENT '事件类型',
        event_details        TEXT         NULL COMMENT '事件详情(JSON)',
        event_time           DATETIME     NOT NULL COMMENT '事件时间',
        PRIMARY KEY (event_id),
        INDEX idx_prod_time (product_id, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 商品运营事件日志';

-- 10. 价格异动日志
DROP TABLE IF EXISTS ods_price_change_log;
CREATE TABLE ods_price_change_log (
       change_id            BIGINT       NOT NULL AUTO_INCREMENT COMMENT '日志ID',
       product_id           BIGINT       NOT NULL COMMENT '商品ID',
       old_price            DECIMAL(10,2) NOT NULL COMMENT '原价',
       new_price            DECIMAL(10,2) NOT NULL COMMENT '新价',
       change_time          DATETIME     NOT NULL COMMENT '变更时间',
       operator_id          BIGINT       NOT NULL COMMENT '操作者ID',
       PRIMARY KEY (change_id),
       INDEX idx_prod_time (product_id, change_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 价格变动日志';

-- 11. 库存快照
DROP TABLE IF EXISTS ods_inventory_snapshot;
CREATE TABLE ods_inventory_snapshot (
         snapshot_id          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '快照ID',
         sku_id               BIGINT       NOT NULL COMMENT 'SKU ID',
         product_id           BIGINT       NOT NULL COMMENT '商品ID',
         inventory_qty        INT          NOT NULL COMMENT '库存数量',
         snapshot_time        DATETIME     NOT NULL COMMENT '快照时间',
         PRIMARY KEY (snapshot_id),
         INDEX idx_sku_time (sku_id, snapshot_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: SKU 库存快照表';

-- 12. 优惠券使用日志
DROP TABLE IF EXISTS ods_coupon_usage_log;
CREATE TABLE ods_coupon_usage_log (
       log_id               BIGINT       NOT NULL AUTO_INCREMENT COMMENT '日志ID',
       coupon_id            BIGINT       NOT NULL COMMENT '优惠券ID',
       user_id              BIGINT       NOT NULL COMMENT '用户ID',
       order_id             BIGINT       NULL COMMENT '使用订单ID',
       product_id           BIGINT       NULL COMMENT '指定商品ID（如有）',
       issue_time           DATETIME     NOT NULL COMMENT '发放时间',
       use_time             DATETIME     NULL COMMENT '使用时间',
       discount_amount      DECIMAL(10,2) NULL COMMENT '优惠金额',
       PRIMARY KEY (log_id),
       INDEX idx_coupon_time (coupon_id, issue_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 优惠券发放与使用日志';

-- 13. 商品微详情页访问日志
DROP TABLE IF EXISTS ods_micro_detail_visit_log;
CREATE TABLE ods_micro_detail_visit_log (
             log_id         BIGINT       NOT NULL AUTO_INCREMENT COMMENT '日志ID',
             user_id        BIGINT       NOT NULL COMMENT '用户ID',
             session_id     VARCHAR(64)  NOT NULL COMMENT '会话ID',
             product_id     BIGINT       NOT NULL COMMENT '商品ID',
             sku_id         BIGINT       NULL COMMENT 'SKU ID',
             event_time     DATETIME     NOT NULL COMMENT '访问时间',
             entry_page     VARCHAR(100) NULL COMMENT '入口页面',
             device_type    VARCHAR(20)  NOT NULL COMMENT '设备类型',
             region         VARCHAR(50)  NULL COMMENT '访问地区',
             PRIMARY KEY (log_id),
             INDEX idx_prod_time (product_id, event_time),
             INDEX idx_user_time (user_id, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 商品微详情页访问日志';

-- 14. 页面停留与跳出日志
DROP TABLE IF EXISTS ods_page_stay_log;
CREATE TABLE ods_page_stay_log (
    log_id         BIGINT       NOT NULL AUTO_INCREMENT COMMENT '日志ID',
    user_id        BIGINT       NOT NULL COMMENT '用户ID',
    session_id     VARCHAR(64)  NOT NULL COMMENT '会话ID',
    page_type      ENUM('PRODUCT_DETAIL','MICRO_DETAIL','HOME','CATEGORY') NOT NULL COMMENT '页面类型',
    product_id     BIGINT       NULL COMMENT '商品ID（如 PRODUCT_DETAIL）',
    entry_time     DATETIME     NOT NULL COMMENT '进入时间',
    exit_time      DATETIME     NOT NULL COMMENT '离开时间',
    stay_seconds   DECIMAL(10,2) NOT NULL COMMENT '停留时长(秒)',
    is_bounce      TINYINT(1)   NOT NULL COMMENT '是否跳出(1=是,0=否)',
    PRIMARY KEY (log_id),
    INDEX idx_page_time (page_type, entry_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 页面停留时长与跳出埋点';
    
-- 15. 用户商品关注/取消关注日志
DROP TABLE IF EXISTS ods_favorite_log;
CREATE TABLE ods_favorite_log (
    log_id         BIGINT       NOT NULL AUTO_INCREMENT COMMENT '日志ID',
    user_id        BIGINT       NOT NULL COMMENT '用户ID',
    product_id     BIGINT       NOT NULL COMMENT '商品ID',
    sku_id         BIGINT       NULL COMMENT 'SKU ID',
    action_time    DATETIME     NOT NULL COMMENT '操作时间',
    action_type    ENUM('FAVOR','UNFAVOR') NOT NULL COMMENT '关注/取消关注',
    PRIMARY KEY (log_id),
    INDEX idx_prod_time (product_id, action_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 商品关注/取消关注日志';

-- 16. 广告点击与投放明细日志
DROP TABLE IF EXISTS ods_ad_click_log;
CREATE TABLE ods_ad_click_log (
    log_id         BIGINT       NOT NULL AUTO_INCREMENT COMMENT '日志ID',
    user_id        BIGINT       NOT NULL COMMENT '用户ID',
    session_id     VARCHAR(64)  NOT NULL COMMENT '会话ID',
    product_id     BIGINT       NULL COMMENT '点击商品ID',
    ad_source      VARCHAR(50)  NOT NULL COMMENT '广告大类(效果/站外/内容/品牌)',
    campaign_id    BIGINT       NULL COMMENT '投放活动ID',
    creative_id    BIGINT       NULL COMMENT '创意素材ID',
    click_time     DATETIME     NOT NULL COMMENT '点击时间',
    cost           DECIMAL(10,4) NULL COMMENT '扣费金额',
    PRIMARY KEY (log_id),
    INDEX idx_src_time (ad_source, click_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 广告点击与投放明细';

-- 17. 营销工具领取/使用日志
DROP TABLE IF EXISTS ods_promotion_usage_log;
CREATE TABLE ods_promotion_usage_log (
          log_id         BIGINT       NOT NULL AUTO_INCREMENT COMMENT '日志ID',
          user_id        BIGINT       NOT NULL COMMENT '用户ID',
          order_id       BIGINT       NULL COMMENT '使用关联订单ID',
          product_id     BIGINT       NULL COMMENT '指定商品ID',
          promotion_type VARCHAR(50)  NOT NULL COMMENT '工具类型(新客折扣/新品礼金/购物金/盒护工具)',
          issue_time     DATETIME     NOT NULL COMMENT '发放时间',
          use_time       DATETIME     NULL COMMENT '使用时间',
          discount_amount DECIMAL(10,2) NULL COMMENT '优惠金额',
          PRIMARY KEY (log_id),
          INDEX idx_promo_time (promotion_type, issue_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS: 营销工具领取与使用日志';

-- ==============================================
-- MySQL 5.7 版本——ODS 表 1,000 条以上模拟数据生成脚本
-- 使用存储过程 + 循环 + RAND() 生成“看起来真实”的随机数据
-- ==============================================

DELIMITER $$


-- 1. 生成 ods_product_info

DROP PROCEDURE IF EXISTS gen_ods_product_info$$
CREATE PROCEDURE gen_ods_product_info()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE ts DATETIME;
    WHILE i <= 1000 DO
            SET ts = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            INSERT INTO ods_product_info
            (product_id, product_name, brand_id, vendor_id,
             first_cat_id, second_cat_id, third_cat_id, create_time)
            VALUES (
                       i + 100000,                                           -- product_id
                       CONCAT('商品_', LPAD(i,4,'0')),                      -- product_name
                       FLOOR(RAND()*50)+1,                                   -- brand_id
                       FLOOR(RAND()*20)+1,                                   -- vendor_id
                       FLOOR(RAND()*5)+1,                                    -- first_cat_id
                       FLOOR(RAND()*20)+1,                                   -- second_cat_id
                       FLOOR(RAND()*100)+1,                                  -- third_cat_id
                       ts
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 2. 生成 ods_sku_info

DROP PROCEDURE IF EXISTS gen_ods_sku_info$$
CREATE PROCEDURE gen_ods_sku_info()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE ts DATETIME;
    WHILE i <= 1000 DO
            SET ts = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            INSERT INTO ods_sku_info
            (sku_id, product_id, sku_name, create_time)
            VALUES (
                       i + 200000,                                          -- sku_id
                       FLOOR(RAND()*1000)+100001,                           -- product_id
                       CONCAT('SKU_', LPAD(i,4,'0')),                       -- sku_name
                       ts
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 3. 生成 ods_order_header

DROP PROCEDURE IF EXISTS gen_ods_order_header$$
CREATE PROCEDURE gen_ods_order_header()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE ot DATETIME;
    DECLARE pt DATETIME;
    WHILE i <= 1000 DO
            SET ot = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            SET pt = IF(RAND()<0.9,
                        DATE_ADD(ot, INTERVAL FLOOR(RAND()*3600) SECOND),
                        NULL
                     );
            INSERT INTO ods_order_header
            (order_id, user_id, order_time, pay_time, order_status,
             store_id, total_amount, total_quantity, coupon_id)
            VALUES (
                       i + 300000,                                         -- order_id
                       FLOOR(RAND()*5000)+1,                               -- user_id
                       ot,                                                 -- order_time
                       pt,                                                 -- pay_time
                       ELT(FLOOR(RAND()*4)+1,'CREATED','PAID','CANCELLED','REFUNDED'),
                       FLOOR(RAND()*100)+1,                                -- store_id
                       ROUND(RAND()*1000+50,2),                            -- total_amount
                       FLOOR(RAND()*10)+1,                                 -- total_quantity
                       IF(RAND()<0.3, FLOOR(RAND()*200)+1, NULL)           -- coupon_id
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 4. 生成 ods_order_item

DROP PROCEDURE IF EXISTS gen_ods_order_item$$
CREATE PROCEDURE gen_ods_order_item()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE qty INT;
    DECLARE price DECIMAL(10,2);
    DECLARE amt DECIMAL(10,2);
    WHILE i <= 1000 DO
            SET qty   = FLOOR(RAND()*3)+1;
            SET price = ROUND(RAND()*490+10,2);
            SET amt   = qty * price;
            INSERT INTO ods_order_item
            (order_id, item_id, product_id, sku_id, quantity, price, amount)
            VALUES (
                       FLOOR(RAND()*1000)+300001,                         -- order_id
                       i + 400000,                                        -- item_id
                       FLOOR(RAND()*1000)+100001,                         -- product_id
                       FLOOR(RAND()*1000)+200001,                         -- sku_id
                       qty,
                       price,
                       amt
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 5. 生成 ods_product_visit_log

DROP PROCEDURE IF EXISTS gen_ods_product_visit_log$$
CREATE PROCEDURE gen_ods_product_visit_log()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE ts DATETIME;
    WHILE i <= 1000 DO
            SET ts = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            INSERT INTO ods_product_visit_log
            (log_id, user_id, session_id, product_id, event_time,
             entry_page, referrer_page, device_type, region)
            VALUES (
                       i + 500000,                                        -- log_id
                       FLOOR(RAND()*5000)+1,                             -- user_id
                       CONCAT('sess_', LPAD(FLOOR(RAND()*999999),6,'0')),-- session_id
                       FLOOR(RAND()*1000)+100001,                        -- product_id
                       ts,
                       ELT(FLOOR(RAND()*3)+1,'home','search','category'),
                       ELT(FLOOR(RAND()*4)+1,'NULL','/home','/search?q=xx','/cat/123'),
                       ELT(FLOOR(RAND()*3)+1,'PC','Mobile','App'),
                       CONCAT('region_', FLOOR(RAND()*10)+1)
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 6. 生成 ods_cart_action_log

DROP PROCEDURE IF EXISTS gen_ods_cart_action_log$$
CREATE PROCEDURE gen_ods_cart_action_log()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE ts DATETIME;
    WHILE i <= 1000 DO
            SET ts = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            INSERT INTO ods_cart_action_log
            (log_id, user_id, session_id, product_id, sku_id, action_time,
             action_type, quantity)
            VALUES (
                       i + 600000,                                        -- log_id
                       FLOOR(RAND()*5000)+1,                             -- user_id
                       CONCAT('sess_', LPAD(FLOOR(RAND()*999999),6,'0')),-- session_id
                       FLOOR(RAND()*1000)+100001,                        -- product_id
                       FLOOR(RAND()*1000)+200001,                        -- sku_id
                       ts,
                       ELT(FLOOR(RAND()*2)+1,'ADD','REMOVE'),
                       FLOOR(RAND()*5)+1
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 7. 生成 ods_search_log

DROP PROCEDURE IF EXISTS gen_ods_search_log$$
CREATE PROCEDURE gen_ods_search_log()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE ts DATETIME;
    WHILE i <= 1000 DO
            SET ts = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            INSERT INTO ods_search_log
            (log_id, user_id, session_id, search_keyword, result_count,
             clicked_product_id, search_time)
            VALUES (
                       i + 700000,                                        -- log_id
                       FLOOR(RAND()*5000)+1,                             -- user_id
                       CONCAT('sess_', LPAD(FLOOR(RAND()*999999),6,'0')),-- session_id
                       CONCAT('kw', LPAD(FLOOR(RAND()*999),3,'0')),      -- search_keyword
                       FLOOR(RAND()*200),                                -- result_count
                       IF(RAND()<0.5, FLOOR(RAND()*1000)+100001, NULL),  -- clicked_product_id
                       ts
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 8. 生成 ods_traffic_source_log

DROP PROCEDURE IF EXISTS gen_ods_traffic_source_log$$
CREATE PROCEDURE gen_ods_traffic_source_log()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE ts DATETIME;
    WHILE i <= 1000 DO
            SET ts = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            INSERT INTO ods_traffic_source_log
            (log_id, session_id, user_id, source_type, visit_time)
            VALUES (
                       i + 800000,                                        -- log_id
                       CONCAT('sess_', LPAD(FLOOR(RAND()*999999),6,'0')),
                       FLOOR(RAND()*5000)+1,
                       ELT(FLOOR(RAND()*4)+1,'organic','ads','referral','direct'),
                       ts
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 9. 生成 ods_product_event_log

DROP PROCEDURE IF EXISTS gen_ods_product_event_log$$
CREATE PROCEDURE gen_ods_product_event_log()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE ts DATETIME;
    WHILE i <= 1000 DO
            SET ts = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            INSERT INTO ods_product_event_log
            (event_id, product_id, user_id, event_type, event_details, event_time)
            VALUES (
                       i + 900000,                                        -- event_id
                       FLOOR(RAND()*1000)+100001,                        -- product_id
                       FLOOR(RAND()*300)+1,                              -- user_id
                       ELT(FLOOR(RAND()*4)+1,'PRICE_UPDATE','PROMO_START','PROMO_END','STOCK_ALERT'),
                       '{}',
                       ts
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 10. 生成 ods_price_change_log

DROP PROCEDURE IF EXISTS gen_ods_price_change_log$$
CREATE PROCEDURE gen_ods_price_change_log()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE oldp DECIMAL(10,2);
    DECLARE newp DECIMAL(10,2);
    DECLARE ts DATETIME;
    WHILE i <= 1000 DO
            SET oldp = ROUND(RAND()*490+10,2);
            SET newp = ROUND(oldp * (RAND()*0.4+0.8),2);
            SET ts   = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                       );
            INSERT INTO ods_price_change_log
            (change_id, product_id, old_price, new_price, change_time, operator_id)
            VALUES (
                       i + 1000000,
                       FLOOR(RAND()*1000)+100001,
                       oldp,
                       newp,
                       ts,
                       FLOOR(RAND()*100)+1
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 11. 生成 ods_inventory_snapshot

DROP PROCEDURE IF EXISTS gen_ods_inventory_snapshot$$
CREATE PROCEDURE gen_ods_inventory_snapshot()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE ts DATETIME;
    WHILE i <= 1000 DO
            SET ts = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            INSERT INTO ods_inventory_snapshot
            (snapshot_id, sku_id, product_id, inventory_qty, snapshot_time)
            VALUES (
                       i + 1100000,
                       FLOOR(RAND()*1000)+200001,
                       FLOOR(RAND()*1000)+100001,
                       FLOOR(RAND()*500),
                       ts
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 12. 生成 ods_coupon_usage_log

DROP PROCEDURE IF EXISTS gen_ods_coupon_usage_log$$
CREATE PROCEDURE gen_ods_coupon_usage_log()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE ts DATETIME;
    DECLARE ut DATETIME;
    DECLARE da DECIMAL(10,2);
    WHILE i <= 1000 DO
            SET ts = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            SET ut = IF(RAND()<0.5,
                        DATE_ADD(ts, INTERVAL FLOOR(RAND()*86400) SECOND),
                        NULL);
            SET da = IF(ut IS NULL, NULL, ROUND(RAND()*100,2));
            INSERT INTO ods_coupon_usage_log
            (log_id, coupon_id, user_id, order_id, product_id,
             issue_time, use_time, discount_amount)
            VALUES (
                       i + 1200000,
                       FLOOR(RAND()*200)+1,
                       FLOOR(RAND()*5000)+1,
                       IF(RAND()<0.7, FLOOR(RAND()*1000)+300001, NULL),
                       IF(RAND()<0.5, FLOOR(RAND()*1000)+100001, NULL),
                       ts,
                       ut,
                       da
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 13. 生成 ods_micro_detail_visit_log

DROP PROCEDURE IF EXISTS gen_ods_micro_detail_visit_log$$
CREATE PROCEDURE gen_ods_micro_detail_visit_log()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE ts DATETIME;
    WHILE i <= 1000 DO
            SET ts = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            INSERT INTO ods_micro_detail_visit_log
            (log_id, user_id, session_id, product_id, sku_id,
             event_time, entry_page, device_type, region)
            VALUES (
                       i + 1300000,
                       FLOOR(RAND()*5000)+1,
                       CONCAT('sess_', LPAD(FLOOR(RAND()*999999),6,'0')),
                       FLOOR(RAND()*1000)+100001,
                       IF(RAND()<0.5, FLOOR(RAND()*1000)+200001, NULL),
                       ts,
                       ELT(FLOOR(RAND()*3)+1,'home','cat','promo'),
                       ELT(FLOOR(RAND()*2)+1,'PC','Mobile'),
                       CONCAT('city_', FLOOR(RAND()*100)+1)
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 14. 生成 ods_page_stay_log

DROP PROCEDURE IF EXISTS gen_ods_page_stay_log$$
CREATE PROCEDURE gen_ods_page_stay_log()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE et DATETIME;
    DECLARE xt DATETIME;
    DECLARE stay_sec INT;
    DECLARE bounce TINYINT;
    DECLARE pid_val BIGINT;
    WHILE i <= 1000 DO
            SET et = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            SET xt = DATE_ADD(et, INTERVAL FLOOR(RAND()*600) SECOND);
            SET stay_sec = TIMESTAMPDIFF(SECOND, et, xt);
            SET bounce   = IF(stay_sec < 10, 1, 0);
            SET pid_val  = IF(RAND()<0.5, FLOOR(RAND()*1000)+100001, NULL);
            INSERT INTO ods_page_stay_log
            (log_id, user_id, session_id, page_type, product_id,
             entry_time, exit_time, stay_seconds, is_bounce)
            VALUES (
                       i + 1400000,
                       FLOOR(RAND()*5000)+1,
                       CONCAT('sess_', LPAD(FLOOR(RAND()*999999),6,'0')),
                       ELT(FLOOR(RAND()*4)+1,'PRODUCT_DETAIL','MICRO_DETAIL','HOME','CATEGORY'),
                       pid_val,
                       et,
                       xt,
                       stay_sec,
                       bounce
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 15. 生成 ods_favorite_log

DROP PROCEDURE IF EXISTS gen_ods_favorite_log$$
CREATE PROCEDURE gen_ods_favorite_log()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE ts DATETIME;
    WHILE i <= 1000 DO
            SET ts = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            INSERT INTO ods_favorite_log
            (log_id, user_id, product_id, sku_id, action_time, action_type)
            VALUES (
                       i + 1500000,
                       FLOOR(RAND()*5000)+1,
                       FLOOR(RAND()*1000)+100001,
                       IF(RAND()<0.5, FLOOR(RAND()*1000)+200001, NULL),
                       ts,
                       ELT(FLOOR(RAND()*2)+1,'FAVOR','UNFAVOR')
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 16. 生成 ods_ad_click_log

DROP PROCEDURE IF EXISTS gen_ods_ad_click_log$$
CREATE PROCEDURE gen_ods_ad_click_log()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE ts DATETIME;
    DECLARE camp BIGINT;
    DECLARE cre BIGINT;
    DECLARE cost_val DECIMAL(10,4);
    WHILE i <= 1000 DO
            SET ts = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            SET camp     = IF(RAND()<0.5, FLOOR(RAND()*1000)+1, NULL);
            SET cre      = IF(RAND()<0.5, FLOOR(RAND()*5000)+1, NULL);
            SET cost_val = ROUND(RAND()*4.9+0.1,4);
            INSERT INTO ods_ad_click_log
            (log_id, user_id, session_id, product_id, ad_source,
             campaign_id, creative_id, click_time, cost)
            VALUES (
                       i + 1600000,
                       FLOOR(RAND()*5000)+1,
                       CONCAT('sess_', LPAD(FLOOR(RAND()*999999),6,'0')),
                       IF(RAND()<0.5, FLOOR(RAND()*1000)+100001, NULL),
                       ELT(FLOOR(RAND()*4)+1,'效果','站外','内容','品牌'),
                       camp,
                       cre,
                       ts,
                       cost_val
                   );
            SET i = i + 1;
        END WHILE;
END$$


-- 17. 生成 ods_promotion_usage_log

DROP PROCEDURE IF EXISTS gen_ods_promotion_usage_log$$
CREATE PROCEDURE gen_ods_promotion_usage_log()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE ts DATETIME;
    DECLARE ut DATETIME;
    DECLARE da DECIMAL(10,2);
    WHILE i <= 1000 DO
            SET ts = DATE_ADD(
                    DATE_ADD('2025-01-01 00:00:00', INTERVAL FLOOR(RAND()*365) DAY),
                    INTERVAL FLOOR(RAND()*86400) SECOND
                     );
            SET ut = IF(RAND()<0.5,
                        DATE_ADD(ts, INTERVAL FLOOR(RAND()*86400) SECOND),
                        NULL
                     );
            SET da = IF(ut IS NULL, NULL, ROUND(RAND()*100,2));
            INSERT INTO ods_promotion_usage_log
            (log_id, user_id, order_id, product_id, promotion_type,
             issue_time, use_time, discount_amount)
            VALUES (
                       i + 1700000,
                       FLOOR(RAND()*5000)+1,
                       IF(RAND()<0.7, FLOOR(RAND()*1000)+300001, NULL),
                       IF(RAND()<0.5, FLOOR(RAND()*1000)+100001, NULL),
                       ELT(FLOOR(RAND()*4)+1,'新客折扣','新品礼金','购物金','盒护工具'),
                       ts,
                       ut,
                       da
                   );
            SET i = i + 1;
        END WHILE;
END$$

DELIMITER ;


CALL gen_ods_product_info();
CALL gen_ods_sku_info();
CALL gen_ods_order_header();
CALL gen_ods_order_item();
CALL gen_ods_product_visit_log();
CALL gen_ods_cart_action_log();
CALL gen_ods_search_log();
CALL gen_ods_traffic_source_log();
CALL gen_ods_product_event_log();
CALL gen_ods_price_change_log();
CALL gen_ods_inventory_snapshot();
CALL gen_ods_coupon_usage_log();
CALL gen_ods_micro_detail_visit_log();
CALL gen_ods_page_stay_log();
CALL gen_ods_favorite_log();
CALL gen_ods_ad_click_log();
CALL gen_ods_promotion_usage_log();


