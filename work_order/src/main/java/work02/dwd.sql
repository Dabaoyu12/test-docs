USE gmall_work_02;

-- 1.支付订单明细（保留已支付记录）
DROP TABLE IF EXISTS dwd_trade_order_detail;
CREATE TABLE dwd_trade_order_detail (
        order_id        BIGINT    NOT NULL COMMENT '订单ID',
        item_id         BIGINT    NOT NULL COMMENT '明细ID',
        user_id         BIGINT    NOT NULL COMMENT '买家ID',
        sku_id          BIGINT    NOT NULL COMMENT 'SKU ID',
        product_id      BIGINT    NOT NULL COMMENT '商品ID',
        quantity        INT       NOT NULL COMMENT '购买数量',
        price           DECIMAL(10,2) NOT NULL COMMENT '成交单价',
        amount          DECIMAL(10,2) NOT NULL COMMENT '成交总额',
        pay_time        DATETIME  NOT NULL COMMENT '支付时间',
        dt              DATE      NOT NULL COMMENT '业务日期（支付日期）',
        INDEX idx_dt (dt),
        INDEX idx_user (user_id),
        INDEX idx_prod (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWD: 支付订单明细';

-- 2.商品访问明细
DROP TABLE IF EXISTS dwd_user_visit_detail;
CREATE TABLE dwd_user_visit_detail (
                                       log_id          BIGINT    NOT NULL COMMENT '日志ID',
                                       user_id         BIGINT    NOT NULL COMMENT '用户ID',
                                       session_id      VARCHAR(64) NOT NULL COMMENT '会话ID',
                                       product_id      BIGINT    NOT NULL COMMENT '商品ID',
                                       event_time      DATETIME  NOT NULL COMMENT '访问时间',
                                       entry_page      VARCHAR(100) COMMENT '入口页面',
                                       referrer_page   VARCHAR(100) COMMENT '来源页面',
                                       device_type     VARCHAR(20) COMMENT '设备类型',
                                       region          VARCHAR(50) COMMENT '访问地区',
                                       dt              DATE      NOT NULL COMMENT '业务日期（访问日期）',
                                       INDEX idx_dt (dt),
                                       INDEX idx_prod (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWD: 商品详情页访问明细';

-- 3. 搜索行为明细
DROP TABLE IF EXISTS dwd_search_keyword_detail;
CREATE TABLE dwd_search_keyword_detail (
           log_id            BIGINT    NOT NULL COMMENT '日志ID',
           user_id           BIGINT    NOT NULL COMMENT '用户ID',
           session_id        VARCHAR(64) NOT NULL COMMENT '会话ID',
           search_keyword    VARCHAR(200) COMMENT '搜索词',
           result_count      INT       COMMENT '命中结果数',
           clicked_product_id BIGINT   COMMENT '点击商品ID',
           search_time       DATETIME  NOT NULL COMMENT '搜索时间',
           dt                DATE      NOT NULL COMMENT '业务日期（搜索日期）',
           INDEX idx_dt (dt),
           INDEX idx_keyword (search_keyword)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWD: 搜索行为明细';

-- 4.流量来源明细
DROP TABLE IF EXISTS dwd_traffic_source_detail;
CREATE TABLE dwd_traffic_source_detail (
           log_id          BIGINT    NOT NULL COMMENT '日志ID',
           session_id      VARCHAR(64) NOT NULL COMMENT '会话ID',
           user_id         BIGINT    NOT NULL COMMENT '用户ID',
           source_type     VARCHAR(50) COMMENT '流量来源渠道',
           visit_time      DATETIME  NOT NULL COMMENT '访问时间',
           dt              DATE      NOT NULL COMMENT '业务日期（访问日期）',
           INDEX idx_dt (dt),
           INDEX idx_source (source_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWD: 流量来源明细';

-- 5. 购物车操作明细
DROP TABLE IF EXISTS dwd_cart_action_detail;
CREATE TABLE dwd_cart_action_detail (
        log_id          BIGINT    NOT NULL COMMENT '日志ID',
        user_id         BIGINT    NOT NULL COMMENT '用户ID',
        session_id      VARCHAR(64) NOT NULL COMMENT '会话ID',
        product_id      BIGINT    NOT NULL COMMENT '商品ID',
        sku_id          BIGINT    NOT NULL COMMENT 'SKU ID',
        action_type     ENUM('ADD','REMOVE') COMMENT '加购/移除',
        quantity        INT       COMMENT '操作件数',
        action_time     DATETIME  NOT NULL COMMENT '操作时间',
        dt              DATE      NOT NULL COMMENT '业务日期',
        INDEX idx_dt (dt),
        INDEX idx_prod (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWD: 购物车操作明细';

-- 6.优惠券使用明细
DROP TABLE IF EXISTS dwd_coupon_usage_detail;
CREATE TABLE dwd_coupon_usage_detail (
         log_id          BIGINT    NOT NULL COMMENT '日志ID',
         coupon_id       BIGINT    COMMENT '优惠券ID',
         user_id         BIGINT    NOT NULL COMMENT '用户ID',
         order_id        BIGINT    COMMENT '使用订单ID',
         product_id      BIGINT    COMMENT '指定商品ID',
         issue_time      DATETIME  NOT NULL COMMENT '发放时间',
         use_time        DATETIME  COMMENT '使用时间',
         discount_amount DECIMAL(10,2) COMMENT '优惠金额',
         dt              DATE      NOT NULL COMMENT '业务日期',
         INDEX idx_dt (dt),
         INDEX idx_coupon (coupon_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWD: 优惠券使用明细';

-- 7.价格变动明细
DROP TABLE IF EXISTS dwd_price_change_detail;
CREATE TABLE dwd_price_change_detail (
         change_id       BIGINT    NOT NULL COMMENT '变更日志ID',
         product_id      BIGINT    NOT NULL COMMENT '商品ID',
         old_price       DECIMAL(10,2) COMMENT '原价',
         new_price       DECIMAL(10,2) COMMENT '新价',
         change_time     DATETIME  NOT NULL COMMENT '变更时间',
         operator_id     BIGINT    COMMENT '操作者ID',
         dt              DATE      NOT NULL COMMENT '业务日期',
         INDEX idx_dt (dt),
         INDEX idx_prod (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWD: 价格变动明细';




INSERT INTO dwd_trade_order_detail
SELECT
    hi.order_id,
    it.item_id,
    hi.user_id,
    it.sku_id,
    it.product_id,
    it.quantity,
    it.price,
    it.amount,
    hi.pay_time,
    DATE(hi.pay_time) AS dt
FROM ods_order_header hi
         JOIN ods_order_item it
              ON hi.order_id = it.order_id
WHERE hi.pay_time IS NOT NULL
  AND hi.order_status = 'PAID';


INSERT INTO dwd_user_visit_detail
SELECT
    log_id,
    user_id,
    session_id,
    product_id,
    event_time,
    entry_page,
    referrer_page,
    device_type,
    region,
    DATE(event_time) AS dt
FROM ods_product_visit_log;


INSERT INTO dwd_search_keyword_detail
SELECT
    log_id,
    user_id,
    session_id,
    search_keyword,
    result_count,
    clicked_product_id,
    search_time,
    DATE(search_time) AS dt
FROM ods_search_log;


INSERT INTO dwd_traffic_source_detail
SELECT
    log_id,
    session_id,
    user_id,
    source_type,
    visit_time,
    DATE(visit_time) AS dt
FROM ods_traffic_source_log;


INSERT INTO dwd_cart_action_detail
SELECT
    log_id,
    user_id,
    session_id,
    product_id,
    sku_id,
    action_type,
    quantity,
    action_time,
    DATE(action_time) AS dt
FROM ods_cart_action_log;


INSERT INTO dwd_coupon_usage_detail
SELECT
    log_id,
    coupon_id,
    user_id,
    order_id,
    product_id,
    issue_time,
    use_time,
    discount_amount,
    DATE(COALESCE(use_time, issue_time)) AS dt
FROM ods_coupon_usage_log;


INSERT INTO dwd_price_change_detail
SELECT
    change_id,
    product_id,
    old_price,
    new_price,
    change_time,
    operator_id,
    DATE(change_time) AS dt
FROM ods_price_change_log;
