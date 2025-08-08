USE gmall_work_02;
    
-- 1. 商品核心指标汇总表
DROP TABLE IF EXISTS ads_product_core_metrics;
CREATE TABLE ads_product_core_metrics (
    product_id        BIGINT       NOT NULL COMMENT '商品ID',
    dt                DATE         NOT NULL COMMENT '统计日期',
    period_type       VARCHAR(10)  NOT NULL COMMENT '统计周期类型(DAILY/WEEKLY/MONTHLY/7DAYS/30DAYS)',
    visit_cnt         BIGINT       NOT NULL DEFAULT 0 COMMENT '商品访客数',
    micro_visit_cnt   BIGINT       NOT NULL DEFAULT 0 COMMENT '商品微详情访客数',
    browse_cnt        BIGINT       NOT NULL DEFAULT 0 COMMENT '商品浏览量',
    avg_stay_duration DECIMAL(10,2) NOT NULL DEFAULT 0.0 COMMENT '平均停留时长(秒)',
    bounce_rate       DECIMAL(5,2) NOT NULL DEFAULT 0.0 COMMENT '跳出率',
    cart_add_cnt      BIGINT       NOT NULL DEFAULT 0 COMMENT '加购人数',
    order_user_cnt    BIGINT       NOT NULL DEFAULT 0 COMMENT '下单人数',
    order_cnt         BIGINT       NOT NULL DEFAULT 0 COMMENT '下单件数',
    order_amount      DECIMAL(10,2) NOT NULL DEFAULT 0.0 COMMENT '下单金额',
    pay_user_cnt      BIGINT       NOT NULL DEFAULT 0 COMMENT '支付买家数',
    pay_cnt           BIGINT       NOT NULL DEFAULT 0 COMMENT '支付件数',
    pay_amount        DECIMAL(10,2) NOT NULL DEFAULT 0.0 COMMENT '支付金额',
    pay_conversion    DECIMAL(5,2) NOT NULL DEFAULT 0.0 COMMENT '支付转化率',
    PRIMARY KEY (product_id, dt, period_type),
    INDEX idx_dt (dt),
    INDEX idx_period (period_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS: 商品核心指标汇总表';

-- 2. 商品流量来源分析表
DROP TABLE IF EXISTS ads_product_traffic_source;
CREATE TABLE ads_product_traffic_source (
      product_id    BIGINT       NOT NULL COMMENT '商品ID',
      dt            DATE         NOT NULL COMMENT '统计日期',
      period_type   VARCHAR(10)  NOT NULL COMMENT '统计周期类型(DAILY/WEEKLY/MONTHLY/7DAYS/30DAYS)',
      source_type   VARCHAR(50)  NOT NULL COMMENT '流量来源渠道',
      visit_cnt     BIGINT       NOT NULL DEFAULT 0 COMMENT '访客数',
      pay_user_cnt  BIGINT       NOT NULL DEFAULT 0 COMMENT '支付买家数',
      pay_conversion DECIMAL(5,2) NOT NULL DEFAULT 0.0 COMMENT '支付转化率',
      PRIMARY KEY (product_id, dt, period_type, source_type),
      INDEX idx_dt (dt),
      INDEX idx_source (source_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS: 商品流量来源分析表';

-- 3. 商品SKU销售分析表
DROP TABLE IF EXISTS ads_product_sku_sales;
CREATE TABLE ads_product_sku_sales (
    product_id    BIGINT       NOT NULL COMMENT '商品ID',
    sku_id        BIGINT       NOT NULL COMMENT 'SKU ID',
    dt            DATE         NOT NULL COMMENT '统计日期',
    period_type   VARCHAR(10)  NOT NULL COMMENT '统计周期类型(DAILY/WEEKLY/MONTHLY/7DAYS/30DAYS)',
    pay_cnt       BIGINT       NOT NULL DEFAULT 0 COMMENT '支付件数',
    pay_amount    DECIMAL(10,2) NOT NULL DEFAULT 0.0 COMMENT '支付金额',
    inventory_qty INT          NOT NULL DEFAULT 0 COMMENT '当前库存',
    stock_days    INT          NOT NULL DEFAULT 0 COMMENT '库存可售天数',
    PRIMARY KEY (product_id, sku_id, dt, period_type),
    INDEX idx_dt (dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS: 商品SKU销售分析表';

-- 4. 商品搜索词分析表
DROP TABLE IF EXISTS ads_product_search_keyword;
CREATE TABLE ads_product_search_keyword (
      product_id     BIGINT       NOT NULL COMMENT '商品ID',
      dt             DATE         NOT NULL COMMENT '统计日期',
      period_type    VARCHAR(10)  NOT NULL COMMENT '统计周期类型(DAILY/WEEKLY/MONTHLY/7DAYS/30DAYS)',
      search_keyword VARCHAR(200) NOT NULL COMMENT '搜索词',
      visit_cnt      BIGINT       NOT NULL DEFAULT 0 COMMENT '访客数',
      click_cnt      BIGINT       NOT NULL DEFAULT 0 COMMENT '点击数',
      pay_user_cnt   BIGINT       NOT NULL DEFAULT 0 COMMENT '支付买家数',
      PRIMARY KEY (product_id, dt, period_type, search_keyword),
      INDEX idx_dt (dt),
      INDEX idx_keyword (search_keyword)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS: 商品搜索词分析表';

-- 5. 价格力商品分析表
DROP TABLE IF EXISTS ads_product_price_force;
CREATE TABLE ads_product_price_force (
      product_id        BIGINT       NOT NULL COMMENT '商品ID',
      dt                DATE         NOT NULL COMMENT '统计日期',
      period_type       VARCHAR(10)  NOT NULL COMMENT '统计周期类型(DAILY/WEEKLY/MONTHLY/7DAYS/30DAYS)',
      price_force_level TINYINT      NOT NULL COMMENT '价格力等级(1-5星)',
      current_price     DECIMAL(10,2) NOT NULL COMMENT '当前价格',
      market_price      DECIMAL(10,2) NOT NULL COMMENT '市场均价',
      pay_conversion    DECIMAL(5,2) NOT NULL DEFAULT 0.0 COMMENT '支付转化率',
      pay_amount        DECIMAL(10,2) NOT NULL DEFAULT 0.0 COMMENT '支付金额',
      pay_cnt           BIGINT       NOT NULL DEFAULT 0 COMMENT '支付件数',
      is_warning        TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '是否预警(0-否,1-是)',
      warning_type      VARCHAR(20)  COMMENT '预警类型(LOW_PRICE_FORCE,LOW_PRODUCT_FORCE)',
      PRIMARY KEY (product_id, dt, period_type),
      INDEX idx_dt (dt),
      INDEX idx_level (price_force_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS: 价格力商品分析表';


-- 3. 从DWS层构建ADS层 - 商品核心指标汇总表 (7天维度)
TRUNCATE TABLE ads_product_core_metrics;
INSERT INTO ads_product_core_metrics
SELECT
    product_id,
    CURDATE() AS dt,
    '7DAYS' AS period_type,
    SUM(visit_cnt) AS visit_cnt,
    SUM(micro_visit_cnt) AS micro_visit_cnt,
    SUM(browse_cnt) AS browse_cnt,
    AVG(avg_stay_duration) AS avg_stay_duration,
    AVG(bounce_rate) AS bounce_rate,
    SUM(cart_add_cnt) AS cart_add_cnt,
    SUM(order_user_cnt) AS order_user_cnt,
    SUM(order_cnt) AS order_cnt,
    SUM(order_amount) AS order_amount,
    SUM(pay_user_cnt) AS pay_user_cnt,
    SUM(pay_cnt) AS pay_cnt,
    SUM(pay_amount) AS pay_amount,
    (SUM(pay_user_cnt) / NULLIF(SUM(visit_cnt), 0)) * 100 AS pay_conversion
FROM dws_product_daily
WHERE dt >= CURDATE() - INTERVAL 7 DAY
  AND dt < CURDATE()
GROUP BY product_id;

-- 商品核心指标汇总表
INSERT INTO ads_product_core_metrics
SELECT
    product_id,
    CURDATE() AS dt,
    '30DAYS' AS period_type,
    SUM(visit_cnt) AS visit_cnt,
    SUM(micro_visit_cnt) AS micro_visit_cnt,
    SUM(browse_cnt) AS browse_cnt,
    AVG(avg_stay_duration) AS avg_stay_duration,
    AVG(bounce_rate) AS bounce_rate,
    SUM(cart_add_cnt) AS cart_add_cnt,
    SUM(order_user_cnt) AS order_user_cnt,
    SUM(order_cnt) AS order_cnt,
    SUM(order_amount) AS order_amount,
    SUM(pay_user_cnt) AS pay_user_cnt,
    SUM(pay_cnt) AS pay_cnt,
    SUM(pay_amount) AS pay_amount,
    (SUM(pay_user_cnt) / NULLIF(SUM(visit_cnt), 0)) * 100 AS pay_conversion
FROM dws_product_daily
WHERE dt >= CURDATE() - INTERVAL 30 DAY
  AND dt < CURDATE()
GROUP BY product_id;

-- 5. 商品核心指标汇总表 (日维度)
INSERT INTO ads_product_core_metrics
SELECT
    product_id,
    dt,
    'DAILY' AS period_type,
    visit_cnt,
    micro_visit_cnt,
    browse_cnt,
    avg_stay_duration,
    bounce_rate,
    cart_add_cnt,
    order_user_cnt,
    order_cnt,
    order_amount,
    pay_user_cnt,
    pay_cnt,
    pay_amount,
    (pay_user_cnt / NULLIF(visit_cnt, 0)) * 100 AS pay_conversion
FROM dws_product_daily
WHERE dt >= CURDATE() - INTERVAL 30 DAY
  AND dt < CURDATE();

-- 6.商品流量来源分析表 (7天维度)
TRUNCATE TABLE ads_product_traffic_source;
INSERT INTO ads_product_traffic_source
SELECT
    product_id,
    CURDATE() AS dt,
    '7DAYS' AS period_type,
    source_type,
    SUM(visit_cnt) AS visit_cnt,
    SUM(pay_user_cnt) AS pay_user_cnt,
    (SUM(pay_user_cnt) / NULLIF(SUM(visit_cnt), 0)) * 100 AS pay_conversion
FROM dws_traffic_daily
WHERE dt >= CURDATE() - INTERVAL 7 DAY
  AND dt < CURDATE()
GROUP BY product_id, source_type;



TRUNCATE TABLE ads_product_sku_sales;
INSERT INTO ads_product_sku_sales
SELECT
    s.product_id,
    s.sku_id,
    CURDATE() AS dt,
    '7DAYS' AS period_type,
    SUM(s.pay_cnt) AS pay_cnt,
    SUM(s.pay_amount) AS pay_amount,
    COALESCE((SELECT inventory_qty
              FROM ods_inventory_snapshot
              WHERE sku_id = s.sku_id
              ORDER BY snapshot_time DESC
              LIMIT 1), 0) AS inventory_qty,
    COALESCE((SELECT inventory_qty
              FROM ods_inventory_snapshot
              WHERE sku_id = s.sku_id
              ORDER BY snapshot_time DESC
              LIMIT 1), 0) /
    NULLIF(AVG(s.pay_cnt), 0) * 7 AS stock_days
FROM dws_sku_daily s
WHERE s.dt >= CURDATE() - INTERVAL 7 DAY
  AND s.dt < CURDATE()
GROUP BY s.product_id, s.sku_id;



TRUNCATE TABLE ads_product_search_keyword;
INSERT INTO ads_product_search_keyword
SELECT
    product_id,
    CURDATE() AS dt,
    '7DAYS' AS period_type,
    search_keyword,
    SUM(visit_cnt) AS visit_cnt,
    SUM(click_cnt) AS click_cnt,
    SUM(pay_user_cnt) AS pay_user_cnt
FROM dws_search_daily
WHERE dt >= CURDATE() - INTERVAL 7 DAY
  AND dt < CURDATE()
GROUP BY product_id, search_keyword;



TRUNCATE TABLE ads_product_price_force;
INSERT INTO ads_product_price_force
SELECT
    p.product_id,
    CURDATE() AS dt,
    '7DAYS' AS period_type,
    p.price_force_level,
    p.current_price,
    p.market_price,
    p.pay_conversion,
    p.pay_amount,
    p.pay_cnt,
    CASE
        WHEN p.pay_conversion < (SELECT AVG(pay_user_cnt / NULLIF(visit_cnt, 0)) * 100
                                 FROM dws_product_daily
                                 WHERE dt >= CURDATE() - INTERVAL 7 DAY
                                   AND dt < CURDATE()) * 0.8
            AND p.pay_conversion < COALESCE((
                                                SELECT AVG(pay_user_cnt / NULLIF(visit_cnt, 0)) * 100
                                                FROM dws_product_daily
                                                WHERE product_id = p.product_id
                                                  AND dt >= CURDATE() - INTERVAL 14 DAY
                                                  AND dt < CURDATE() - INTERVAL 7 DAY
                                            ), 0) THEN 1
        ELSE 0
        END AS is_warning,
    CASE
        WHEN p.pay_conversion < (SELECT AVG(pay_user_cnt / NULLIF(visit_cnt, 0)) * 100
                                 FROM dws_product_daily
                                 WHERE dt >= CURDATE() - INTERVAL 7 DAY
                                   AND dt < CURDATE()) * 0.8
            AND p.pay_conversion < COALESCE((
                                                SELECT AVG(pay_user_cnt / NULLIF(visit_cnt, 0)) * 100
                                                FROM dws_product_daily
                                                WHERE product_id = p.product_id
                                                  AND dt >= CURDATE() - INTERVAL 14 DAY
                                                  AND dt < CURDATE() - INTERVAL 7 DAY
                                            ), 0) THEN 'LOW_PRODUCT_FORCE'
        ELSE NULL
        END AS warning_type
FROM (
         SELECT
             product_id,
             price_force_level,
             current_price,
             market_price,
             AVG(pay_conversion) AS pay_conversion,
             SUM(pay_amount) AS pay_amount,
             SUM(pay_cnt) AS pay_cnt
         FROM dws_price_force_daily
         WHERE dt >= CURDATE() - INTERVAL 7 DAY
           AND dt < CURDATE()
         GROUP BY product_id, price_force_level, current_price, market_price
     ) p;