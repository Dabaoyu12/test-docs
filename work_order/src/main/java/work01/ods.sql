use gmall_work_01;
drop table if exists ods_event_item_visit;
CREATE TABLE ods_event_item_visit (
                                      visit_id        BIGINT       NOT NULL AUTO_INCREMENT COMMENT '事件主键',
                                      user_id         BIGINT       NOT NULL                COMMENT '访客买家ID',
                                      item_id         BIGINT       NOT NULL                COMMENT '商品ID',
                                      date_key        INT          NOT NULL                COMMENT '访问日期键(YYYYMMDD)',
                                      visit_time      DATETIME     NOT NULL                COMMENT '访问时间戳',
                                      dwell_seconds   INT          DEFAULT NULL            COMMENT '停留时长(秒)',
                                      terminal        ENUM('PC','Wireless','App') NOT NULL COMMENT '访问终端',
                                      etl_time        DATETIME     NOT NULL                COMMENT 'ODS 写入时间',
                                      PRIMARY KEY (visit_id),
                                      KEY idx_visit_date   (date_key),
                                      KEY idx_visit_item   (item_id),
                                      KEY idx_visit_user   (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 商品访问及停留日志';


-- ============================================================================
-- 2. 商品详情页浏览 (PV) 日志
-- ----------------------------------------------------------------------------
-- 用于统计详情页浏览量、独立访客PV等指标
-- ============================================================================
drop table if exists ods_event_item_pv;
CREATE TABLE ods_event_item_pv (
                                   pv_id           BIGINT       NOT NULL AUTO_INCREMENT COMMENT '事件主键',
                                   user_id         BIGINT       NOT NULL                COMMENT '访客买家ID',
                                   item_id         BIGINT       NOT NULL                COMMENT '商品ID',
                                   date_key        INT          NOT NULL                COMMENT '浏览日期键(YYYYMMDD)',
                                   pv_time         DATETIME     NOT NULL                COMMENT '浏览时间戳',
                                   terminal        ENUM('PC','Wireless','App') NOT NULL COMMENT '终端类型',
                                   etl_time        DATETIME     NOT NULL                COMMENT 'ODS 写入时间',
                                   PRIMARY KEY (pv_id),
                                   KEY idx_pv_date      (date_key),
                                   KEY idx_pv_item      (item_id),
                                   KEY idx_pv_user      (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 商品详情页 PV 日志';


-- ============================================================================
-- 3. 商品收藏事件日志
-- ----------------------------------------------------------------------------
-- 记录新增收藏/取消收藏，支持收藏人数、收藏率等指标
-- ============================================================================
drop table if exists ods_event_item_favorite;
CREATE TABLE ods_event_item_favorite (
                                         fav_id          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '事件主键',
                                         user_id         BIGINT       NOT NULL                COMMENT '买家ID',
                                         item_id         BIGINT       NOT NULL                COMMENT '商品ID',
                                         date_key        INT          NOT NULL                COMMENT '事件日期键(YYYYMMDD)',
                                         fav_time        DATETIME     NOT NULL                COMMENT '收藏时间戳',
                                         action_type     ENUM('ADD','REMOVE') NOT NULL      COMMENT '动作类型：ADD=收藏,REMOVE=取消收藏',
                                         etl_time        DATETIME     NOT NULL                COMMENT 'ODS 写入时间',
                                         PRIMARY KEY (fav_id),
                                         KEY idx_fav_date     (date_key),
                                         KEY idx_fav_item     (item_id),
                                         KEY idx_fav_user     (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 商品收藏事件日志';


-- ============================================================================
-- 4. 商品加购/移除购物车日志
-- ----------------------------------------------------------------------------
-- 支持加购人数、加购件数、加购转化率等指标
-- ============================================================================
drop table if exists ods_event_item_cart;
CREATE TABLE ods_event_item_cart (
                                     cart_id         BIGINT       NOT NULL AUTO_INCREMENT COMMENT '事件主键',
                                     user_id         BIGINT       NOT NULL                COMMENT '买家ID',
                                     item_id         BIGINT       NOT NULL                COMMENT '商品ID',
                                     date_key        INT          NOT NULL                COMMENT '事件日期键(YYYYMMDD)',
                                     cart_time       DATETIME     NOT NULL                COMMENT '加购/移除时间戳',
                                     quantity        INT          NOT NULL DEFAULT 1      COMMENT '加购/移除件数',
                                     action_type     ENUM('ADD','REMOVE') NOT NULL      COMMENT '动作类型：ADD=加购,REMOVE=移除',
                                     etl_time        DATETIME     NOT NULL                COMMENT 'ODS 写入时间',
                                     PRIMARY KEY (cart_id),
                                     KEY idx_cart_date    (date_key),
                                     KEY idx_cart_item    (item_id),
                                     KEY idx_cart_user    (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 商品加购/移除购物车日志';


-- ============================================================================
-- 5. 拍下（下单）日志
-- ----------------------------------------------------------------------------
-- 记录拍下件数与金额，支持下单买家数、件单价、下单转化率等指标
-- ============================================================================
drop table if exists ods_event_order;
CREATE TABLE ods_event_order (
                                 order_id        BIGINT       NOT NULL                COMMENT '订单ID',
                                 item_id         BIGINT       NOT NULL                COMMENT '商品ID',
                                 user_id         BIGINT       NOT NULL                COMMENT '买家ID',
                                 date_key        INT          NOT NULL                COMMENT '下单日期键(YYYYMMDD)',
                                 order_time      DATETIME     NOT NULL                COMMENT '下单时间戳',
                                 quantity        INT          NOT NULL                COMMENT '拍下件数',
                                 order_amount    DECIMAL(12,2) NOT NULL               COMMENT '拍下金额',
                                 order_status    VARCHAR(20)  NOT NULL                COMMENT '订单状态',
                                 etl_time        DATETIME     NOT NULL                COMMENT 'ODS 写入时间',
                                 PRIMARY KEY (order_id, item_id),
                                 KEY idx_order_date    (date_key),
                                 KEY idx_order_item    (item_id),
                                 KEY idx_order_user    (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 下单事件日志';


-- ============================================================================
-- 6. 支付日志
-- ----------------------------------------------------------------------------
-- 记录支付金额、支付买家数、预售/聚划算、支付渠道等指标
-- ============================================================================
drop table if exists ods_event_payment;
CREATE TABLE ods_event_payment (
                                   payment_id      BIGINT       NOT NULL AUTO_INCREMENT COMMENT '支付流水ID',
                                   order_id        BIGINT       NOT NULL                COMMENT '关联订单ID',
                                   item_id         BIGINT       NOT NULL                COMMENT '商品ID',
                                   user_id         BIGINT       NOT NULL                COMMENT '买家ID',
                                   date_key        INT          NOT NULL                COMMENT '支付日期键(YYYYMMDD)',
                                   payment_time    DATETIME     NOT NULL                COMMENT '支付时间戳',
                                   payment_amount  DECIMAL(12,2) NOT NULL               COMMENT '支付金额',
                                   payment_method  VARCHAR(64)  NOT NULL                COMMENT '支付方式',
                                   is_presale      TINYINT(1)   NOT NULL DEFAULT 0      COMMENT '是否预售',
                                   presale_stage   VARCHAR(32)  DEFAULT NULL            COMMENT '预售阶段',
                                   is_juhuasuan    TINYINT(1)   NOT NULL DEFAULT 0      COMMENT '是否聚划算',
                                   etl_time        DATETIME     NOT NULL                COMMENT 'ODS 写入时间',
                                   PRIMARY KEY (payment_id),
                                   KEY idx_pay_date     (date_key),
                                   KEY idx_pay_item     (item_id),
                                   KEY idx_pay_user     (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 支付事件日志';


-- ============================================================================
-- 7. 退款/退货日志
-- ----------------------------------------------------------------------------
-- 记录退款金额、类型，支持退款率、退款金额统计等指标
-- ============================================================================
drop table if exists ods_event_refund;
CREATE TABLE ods_event_refund (
                                  refund_id       BIGINT       NOT NULL AUTO_INCREMENT COMMENT '退款流水ID',
                                  payment_id      BIGINT       NOT NULL                COMMENT '关联支付ID',
                                  order_id        BIGINT       NOT NULL                COMMENT '关联订单ID',
                                  item_id         BIGINT       NOT NULL                COMMENT '商品ID',
                                  user_id         BIGINT       NOT NULL                COMMENT '买家ID',
                                  date_key        INT          NOT NULL                COMMENT '退款日期键(YYYYMMDD)',
                                  refund_time     DATETIME     NOT NULL                COMMENT '退款时间戳',
                                  refund_amount   DECIMAL(12,2) NOT NULL               COMMENT '退款金额',
                                  refund_type     ENUM('ONLY_REFUND','RETURN_REFUND') NOT NULL COMMENT '退款类型',
                                  etl_time        DATETIME     NOT NULL                COMMENT 'ODS 写入时间',
                                  PRIMARY KEY (refund_id),
                                  KEY idx_refund_date  (date_key),
                                  KEY idx_refund_item  (item_id),
                                  KEY idx_refund_user  (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 退款/退货事件日志';


-- ============================================================================
-- 8. 商品微详情页浏览日志
-- ----------------------------------------------------------------------------
-- 记录微详情页停留超过3秒的用户，支持微详情访客数指标
-- ============================================================================
drop table if exists ods_event_item_micro_detail;
CREATE TABLE ods_event_item_micro_detail (
                                             micro_id        BIGINT       NOT NULL AUTO_INCREMENT COMMENT '事件主键',
                                             user_id         BIGINT       NOT NULL                COMMENT '访客买家ID',
                                             item_id         BIGINT       NOT NULL                COMMENT '商品ID',
                                             date_key        INT          NOT NULL                COMMENT '浏览日期键(YYYYMMDD)',
                                             start_time      DATETIME     NOT NULL                COMMENT '开始浏览时间',
                                             duration_sec    INT          NOT NULL                COMMENT '浏览时长(秒)',
                                             terminal        ENUM('PC','Wireless','App') NOT NULL COMMENT '终端类型',
                                             etl_time        DATETIME     NOT NULL                COMMENT 'ODS 写入时间',
                                             PRIMARY KEY (micro_id),
                                             KEY idx_micro_date  (date_key),
                                             KEY idx_micro_item  (item_id),
                                             KEY idx_micro_user  (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 商品微详情页浏览日志';

-- =====================================================================
-- 9. 列表页曝光与点击日志
-- 用于统计列表页（搜索/频道/推荐）商品曝光量、点击量、CTR 等指标
-- =====================================================================
drop table if exists ods_event_listing;
CREATE TABLE ods_event_listing (
                                   event_id        BIGINT       NOT NULL AUTO_INCREMENT COMMENT '事件主键',
                                   user_id         BIGINT       DEFAULT NULL           COMMENT '买家ID，访客可为空',
                                   item_id         BIGINT       NOT NULL               COMMENT '商品ID',
                                   date_key        INT          NOT NULL               COMMENT '事件日期键(YYYYMMDD)',
                                   event_time      DATETIME     NOT NULL               COMMENT '事件时间戳',
                                   page_type       ENUM('SEARCH','CATEGORY','RECOMMEND') NOT NULL COMMENT '页面类型',
                                   action_type     ENUM('EXPOSE','CLICK') NOT NULL     COMMENT '曝光/点击',
                                   keyword         VARCHAR(128) DEFAULT NULL           COMMENT '搜索词，仅搜索页曝光/点击记录',
                                   referrer        VARCHAR(256) DEFAULT NULL           COMMENT '来源页URL或渠道',
                                   etl_time        DATETIME     NOT NULL               COMMENT 'ODS 写入时间',
                                   PRIMARY KEY (event_id),
                                   KEY idx_list_date   (date_key),
                                   KEY idx_list_item   (item_id),
                                   KEY idx_list_user   (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 列表页曝光与点击日志';


-- =====================================================================
-- 10. 搜索日志
-- 用于统计搜索词热度、搜索转化率等
-- =====================================================================
drop table if exists ods_event_search;
CREATE TABLE ods_event_search (
                                  search_id       BIGINT       NOT NULL AUTO_INCREMENT COMMENT '事件主键',
                                  user_id         BIGINT       DEFAULT NULL           COMMENT '买家ID，访客可为空',
                                  date_key        INT          NOT NULL               COMMENT '搜索日期键(YYYYMMDD)',
                                  search_time     DATETIME     NOT NULL               COMMENT '搜索时间戳',
                                  keyword         VARCHAR(128) NOT NULL               COMMENT '搜索关键词',
                                  result_count    INT          NOT NULL               COMMENT '搜索结果总数',
                                  click_count     INT          NOT NULL DEFAULT 0     COMMENT '搜索后点击商品次数',
                                  etl_time        DATETIME     NOT NULL               COMMENT 'ODS 写入时间',
                                  PRIMARY KEY (search_id),
                                  KEY idx_search_date (date_key),
                                  KEY idx_search_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 用户搜索日志';


-- =====================================================================
-- 11. 优惠券领取与使用日志
-- 用于统计券的发放量、使用量及使用率
-- =====================================================================
drop table if exists ods_event_coupon;
CREATE TABLE ods_event_coupon (
                                  coupon_event_id BIGINT       NOT NULL AUTO_INCREMENT COMMENT '事件主键',
                                  user_id         BIGINT       NOT NULL               COMMENT '买家ID',
                                  coupon_id       BIGINT       NOT NULL               COMMENT '优惠券ID',
                                  date_key        INT          NOT NULL               COMMENT '事件日期键(YYYYMMDD)',
                                  event_time      DATETIME     NOT NULL               COMMENT '领取/使用时间',
                                  action_type     ENUM('RECEIVE','USE') NOT NULL      COMMENT '动作类型',
                                  order_id        BIGINT       DEFAULT NULL           COMMENT '使用且订单ID',
                                  etl_time        DATETIME     NOT NULL               COMMENT 'ODS 写入时间',
                                  PRIMARY KEY (coupon_event_id),
                                  KEY idx_coupon_date (date_key),
                                  KEY idx_coupon_user (user_id),
                                  KEY idx_coupon_id   (coupon_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 优惠券领取与使用日志';


-- =====================================================================
-- 12. 商品分享日志
-- 用于统计分享次数及渠道分布
-- =====================================================================
drop table if exists ods_event_share;
CREATE TABLE ods_event_share (
                                 share_id        BIGINT       NOT NULL AUTO_INCREMENT COMMENT '事件主键',
                                 user_id         BIGINT       NOT NULL               COMMENT '分享者买家ID',
                                 item_id         BIGINT       NOT NULL               COMMENT '商品ID',
                                 date_key        INT          NOT NULL               COMMENT '分享日期键(YYYYMMDD)',
                                 share_time      DATETIME     NOT NULL               COMMENT '分享时间戳',
                                 channel         VARCHAR(32)  NOT NULL               COMMENT '分享渠道，如微信、微博',
                                 etl_time        DATETIME     NOT NULL               COMMENT 'ODS 写入时间',
                                 PRIMARY KEY (share_id),
                                 KEY idx_share_date (date_key),
                                 KEY idx_share_item (item_id),
                                 KEY idx_share_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 商品分享日志';


-- =====================================================================
-- 13. 店铺收藏与访问日志
-- 用于统计店铺收藏人数、访问人数及渠道
-- =====================================================================
drop table if exists ods_event_shop;
CREATE TABLE ods_event_shop (
                                shop_event_id   BIGINT       NOT NULL AUTO_INCREMENT COMMENT '事件主键',
                                user_id         BIGINT       NOT NULL               COMMENT '买家ID',
                                shop_id         BIGINT       NOT NULL               COMMENT '店铺ID',
                                date_key        INT          NOT NULL               COMMENT '事件日期键(YYYYMMDD)',
                                event_time      DATETIME     NOT NULL               COMMENT '事件时间戳',
                                action_type     ENUM('VISIT','FAVORITE','UNFAVORITE') NOT NULL COMMENT '访问/收藏/取消',
                                referrer        VARCHAR(256) DEFAULT NULL           COMMENT '来源渠道或页面',
                                etl_time        DATETIME     NOT NULL               COMMENT 'ODS 写入时间',
                                PRIMARY KEY (shop_event_id),
                                KEY idx_shop_date  (date_key),
                                KEY idx_shop_user  (user_id),
                                KEY idx_shop_id    (shop_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 店铺访问与收藏日志';


-- =====================================================================
-- 14. 交易关闭与支付失败日志
-- 用于区分用户放弃支付、超时关闭等场景
-- =====================================================================
drop table if exists ods_event_trade_status;
CREATE TABLE ods_event_trade_status (
                                        status_id       BIGINT       NOT NULL AUTO_INCREMENT COMMENT '事件主键',
                                        order_id        BIGINT       NOT NULL               COMMENT '订单ID',
                                        user_id         BIGINT       NOT NULL               COMMENT '买家ID',
                                        date_key        INT          NOT NULL               COMMENT '事件日期键(YYYYMMDD)',
                                        event_time      DATETIME     NOT NULL               COMMENT '事件时间戳',
                                        status_type     ENUM('CLOSE_TIMEOUT','CLOSE_USER','PAY_FAIL') NOT NULL COMMENT '类型',
                                        remark          VARCHAR(256) DEFAULT NULL           COMMENT '失败原因或备注',
                                        etl_time        DATETIME     NOT NULL               COMMENT 'ODS 写入时间',
                                        PRIMARY KEY (status_id),
                                        KEY idx_status_date (date_key),
                                        KEY idx_status_user (user_id),
                                        KEY idx_status_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 交易关闭与支付失败日志';


-- =====================================================================
-- 15. 物流时效与签收日志
-- 用于统计发货、揽件、运输、签收各环节时长
-- =====================================================================
drop table if exists ods_event_logistics;
CREATE TABLE ods_event_logistics (
                                     logistics_id    BIGINT       NOT NULL AUTO_INCREMENT COMMENT '事件主键',
                                     order_id        BIGINT       NOT NULL               COMMENT '订单ID',
                                     date_key        INT          NOT NULL               COMMENT '事件日期键(YYYYMMDD)',
                                     event_time      DATETIME     NOT NULL               COMMENT '事件时间戳',
                                     stage           ENUM('SHIP','PICKUP','IN_TRANSIT','DELIVERED') NOT NULL COMMENT '阶段',
                                     location        VARCHAR(128) DEFAULT NULL           COMMENT '当前地或网点',
                                     etl_time        DATETIME     NOT NULL               COMMENT 'ODS 写入时间',
                                     PRIMARY KEY (logistics_id),
                                     KEY idx_logi_date  (date_key),
                                     KEY idx_logi_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='ODS 原始 – 物流节点事件日志';



DELIMITER //
-- =====================================================================
-- 1. 商品访问及停留日志 ods_event_item_visit
-- =====================================================================
CREATE PROCEDURE  fill_item_visit()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 1000 DO
            INSERT INTO ods_event_item_visit
            (user_id, item_id, date_key, visit_time, dwell_seconds, terminal, etl_time)
            VALUES
                (
                    FLOOR(1 + RAND()*5000),
                    FLOOR(1 + RAND()*2000),
                    DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                    DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                    FLOOR(RAND()*600),
                    ELT(FLOOR(1 + RAND()*3), 'PC','Wireless','App'),
                    NOW()
                );
            SET i = i + 1;
        END WHILE;
END;
//

-- =====================================================================
-- 2. 商品详情页浏览 (PV) 日志 ods_event_item_pv
-- =====================================================================
CREATE PROCEDURE fill_item_pv()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 1000 DO
            INSERT INTO ods_event_item_pv
            (user_id, item_id, date_key, pv_time, terminal, etl_time)
            VALUES
                (
                    FLOOR(1 + RAND()*5000),
                    FLOOR(1 + RAND()*2000),
                    DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                    DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                    ELT(FLOOR(1 + RAND()*3), 'PC','Wireless','App'),
                    NOW()
                );
            SET i = i + 1;
        END WHILE;
END;
//

-- =====================================================================
-- 3. 商品收藏事件日志 ods_event_item_favorite
-- =====================================================================
CREATE PROCEDURE fill_item_fav()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 1000 DO
            INSERT INTO ods_event_item_favorite
            (user_id, item_id, date_key, fav_time, action_type, etl_time)
            VALUES
                (
                    FLOOR(1 + RAND()*5000),
                    FLOOR(1 + RAND()*2000),
                    DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                    DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                    ELT(FLOOR(1 + RAND()*2), 'ADD','REMOVE'),
                    NOW()
                );
            SET i = i + 1;
        END WHILE;
END;
//

-- =====================================================================
-- 4. 商品加购/移除购物车日志 ods_event_item_cart
-- =====================================================================
CREATE PROCEDURE fill_item_cart()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 1000 DO
            INSERT INTO ods_event_item_cart
            (user_id, item_id, date_key, cart_time, quantity, action_type, etl_time)
            VALUES
                (
                    FLOOR(1 + RAND()*5000),
                    FLOOR(1 + RAND()*2000),
                    DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                    DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                    FLOOR(1 + RAND()*5),
                    ELT(FLOOR(1 + RAND()*2), 'ADD','REMOVE'),
                    NOW()
                );
            SET i = i + 1;
        END WHILE;
END;
//

-- =====================================================================
-- 5. 拍下（下单）日志 ods_event_order
-- =====================================================================
CREATE PROCEDURE fill_event_order()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 1000 DO
            INSERT INTO ods_event_order
            (order_id, item_id, user_id, date_key, order_time, quantity, order_amount, order_status, etl_time)
            VALUES
                (
                    i + 100000,
                    FLOOR(1 + RAND()*2000),
                    FLOOR(1 + RAND()*5000),
                    DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                    DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                    FLOOR(1 + RAND()*3),
                    ROUND(RAND()*500+20,2),
                    ELT(FLOOR(1 + RAND()*3), 'CREATED','PAID','CANCELLED'),
                    NOW()
                );
            SET i = i + 1;
        END WHILE;
END;
//

-- =====================================================================
-- 6. 支付日志 ods_event_payment
-- =====================================================================
CREATE PROCEDURE fill_event_payment()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 1000 DO
            INSERT INTO ods_event_payment
            (order_id, item_id, user_id, date_key, payment_time, payment_amount, payment_method, is_presale, presale_stage, is_juhuasuan, etl_time)
            VALUES
                (
                    FLOOR(100000 + RAND()*1000),
                    FLOOR(1 + RAND()*2000),
                    FLOOR(1 + RAND()*5000),
                    DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                    DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                    ROUND(RAND()*500+20,2),
                    ELT(FLOOR(1+RAND()*4), 'ALIPAY','WECHAT','CCB','OTHER'),
                    FLOOR(RAND()+0.5),
                    ELT(FLOOR(1+RAND()*3), 'DEPOSIT','FINAL','ALL'),
                    FLOOR(RAND()+0.5),
                    NOW()
                );
            SET i = i + 1;
        END WHILE;
END;
//

-- =====================================================================
-- 7. 退款/退货日志 ods_event_refund
-- =====================================================================
CREATE PROCEDURE fill_event_refund()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 1000 DO
            INSERT INTO ods_event_refund
            (payment_id, order_id, item_id, user_id, date_key, refund_time, refund_amount, refund_type, etl_time)
            VALUES
                (
                    FLOOR(1000 + RAND()*1000),
                    FLOOR(100000 + RAND()*1000),
                    FLOOR(1 + RAND()*2000),
                    FLOOR(1 + RAND()*5000),
                    DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                    DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                    ROUND(RAND()*300+5,2),
                    ELT(FLOOR(1 + RAND()*2), 'ONLY_REFUND','RETURN_REFUND'),
                    NOW()
                );
            SET i = i + 1;
        END WHILE;
END;
//

-- =====================================================================
-- 8. 商品微详情页浏览日志 ods_event_item_micro_detail
-- =====================================================================
CREATE PROCEDURE fill_micro_detail()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 1000 DO
            INSERT INTO ods_event_item_micro_detail
            (user_id, item_id, date_key, start_time, duration_sec, terminal, etl_time)
            VALUES
                (
                    FLOOR(1 + RAND()*5000),
                    FLOOR(1 + RAND()*2000),
                    DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                    DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                    FLOOR(3 + RAND()*300),
                    ELT(FLOOR(1 + RAND()*3), 'PC','Wireless','App'),
                    NOW()
                );
            SET i = i + 1;
        END WHILE;
END;
//

-- =====================================================================
-- 9. 列表页曝光与点击日志 ods_event_listing
-- =====================================================================
CREATE PROCEDURE fill_event_listing()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 1000 DO
            INSERT INTO ods_event_listing
            (user_id, item_id, date_key, event_time, page_type, action_type, keyword, referrer, etl_time)
            VALUES
                (
                    IF(RAND()>0.2, FLOOR(1+RAND()*5000), NULL),
                    FLOOR(1 + RAND()*2000),
                    DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                    DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                    ELT(FLOOR(1 + RAND()*3),'SEARCH','CATEGORY','RECOMMEND'),
                    ELT(FLOOR(1 + RAND()*2),'EXPOSE','CLICK'),
                    IF(RAND()<0.5, CONCAT('关键词', FLOOR(RAND()*100)), NULL),
                    CONCAT('https://referrer.com/page', FLOOR(RAND()*10)),
                    NOW()
                );
            SET i = i + 1;
        END WHILE;
END;
//

-- =====================================================================
-- 10. 用户搜索日志 ods_event_search
-- =====================================================================
CREATE PROCEDURE fill_event_search()
BEGIN
    DECLARE i INT DEFAULT 1;
            WHILE i <= 1000 DO
                    INSERT INTO ods_event_search
                    (user_id, date_key, search_time, keyword, result_count, click_count, etl_time)
                    VALUES
                        (
                            IF(RAND()>0.2, FLOOR(1+RAND()*5000), NULL),
                            DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                            DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                            CONCAT('词', FLOOR(RAND()*1000)),
                            FLOOR(RAND()*500),
                            FLOOR(RAND()*10),
                            NOW()
                        );
                    SET i = i + 1;
                END WHILE;
        END;
    //

    -- =====================================================================
-- 11. 优惠券领取与使用日志 ods_event_coupon
-- =====================================================================
    CREATE PROCEDURE fill_event_coupon()
    BEGIN
        DECLARE i INT DEFAULT 1;
        WHILE i <= 1000 DO
                INSERT INTO ods_event_coupon
                (user_id, coupon_id, date_key, event_time, action_type, order_id, etl_time)
                VALUES
                    (
                        FLOOR(1 + RAND()*5000),
                        FLOOR(1 + RAND()*100),
                        DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                        DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                        ELT(FLOOR(1+RAND()*2),'RECEIVE','USE'),
                        IF(RAND()>0.5, FLOOR(100000+RAND()*1000), NULL),
                        NOW()
                    );
                SET i = i + 1;
            END WHILE;
    END;
    //

    -- =====================================================================
-- 12. 商品分享日志 ods_event_share
-- =====================================================================
    CREATE PROCEDURE fill_event_share()
    BEGIN
        DECLARE i INT DEFAULT 1;
        WHILE i <= 1000 DO
                INSERT INTO ods_event_share
                (user_id, item_id, date_key, share_time, channel, etl_time)
                VALUES
                    (
                        FLOOR(1 + RAND()*5000),
                        FLOOR(1 + RAND()*2000),
                        DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                        DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                        ELT(FLOOR(1+RAND()*3),'WeChat','Weibo','QQ'),
                        NOW()
                    );
                SET i = i + 1;
            END WHILE;
    END;
    //

    -- =====================================================================
-- 13. 店铺访问与收藏日志 ods_event_shop
-- =====================================================================
    CREATE PROCEDURE fill_event_shop()
    BEGIN
        DECLARE i INT DEFAULT 1;
        WHILE i <= 1000 DO
                INSERT INTO ods_event_shop
                (user_id, shop_id, date_key, event_time, action_type, referrer, etl_time)
                VALUES
                    (
                        FLOOR(1 + RAND()*5000),
                        FLOOR(1 + RAND()*500),
                        DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                        DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                        ELT(FLOOR(1+RAND()*3),'VISIT','FAVORITE','UNFAVORITE'),
                        CONCAT('https://shopref.com/', FLOOR(RAND()*10)),
                NOW()
            );
        SET i = i + 1;
    END WHILE;
END;
//

-- =====================================================================
-- 14. 交易关闭与支付失败日志 ods_event_trade_status
-- =====================================================================
CREATE PROCEDURE fill_event_trade_status()
BEGIN
    DECLARE i INT DEFAULT 1;
                WHILE i <= 1000 DO
                        INSERT INTO ods_event_trade_status
                        (order_id, user_id, date_key, event_time, status_type, remark, etl_time)
                        VALUES
                            (
                                FLOOR(100000 + RAND()*1000),
                                FLOOR(1 + RAND()*5000),
                                DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                                DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                                ELT(FLOOR(1+RAND()*3),'CLOSE_TIMEOUT','CLOSE_USER','PAY_FAIL'),
                                IF(RAND()>0.5, '用户主动关闭', '支付渠道失败'),
                                NOW()
                            );
                        SET i = i + 1;
                    END WHILE;
            END;
        //

        -- =====================================================================
-- 15. 物流节点事件日志 ods_event_logistics
-- =====================================================================
        CREATE PROCEDURE fill_event_logistics()
        BEGIN
            DECLARE i INT DEFAULT 1;
            WHILE i <= 1000 DO
                    INSERT INTO ods_event_logistics
                    (order_id, date_key, event_time, stage, location, etl_time)
                    VALUES
                        (
                            FLOOR(100000 + RAND()*1000),
                            DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), '%Y%m%d')+0,
                            DATE_ADD(DATE_SUB(CURDATE(), INTERVAL FLOOR(RAND()*30) DAY), INTERVAL FLOOR(RAND()*86400) SECOND),
                            ELT(FLOOR(1+RAND()*4),'SHIP','PICKUP','IN_TRANSIT','DELIVERED'),
                            CONCAT('Hub-', FLOOR(RAND()*50)),
                            NOW()
                        );
                    SET i = i + 1;
                END WHILE;
        END;
        //

-- 恢复分隔符
DELIMITER ;

-- 调用各表填充存储过程
CALL fill_item_visit();
CALL fill_item_pv();
CALL fill_item_fav();
CALL fill_item_cart();
CALL fill_event_order();
CALL fill_event_payment();
CALL fill_event_refund();
CALL fill_micro_detail();
CALL fill_event_listing();
CALL fill_event_search();
CALL fill_event_coupon();
CALL fill_event_share();
CALL fill_event_shop();
CALL fill_event_trade_status();
CALL fill_event_logistics();