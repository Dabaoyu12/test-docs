USE gmall_work_02;

-- 1.商品日销售概览
DROP TABLE IF EXISTS dws_goods_sale_1d;
CREATE TABLE dws_goods_sale_1d (
    dt                   DATE        NOT NULL COMMENT '业务日期',
    product_id           BIGINT      NOT NULL COMMENT '商品ID',
    first_cat_id         INT         COMMENT '一级类目ID',
    second_cat_id        INT         COMMENT '二级类目ID',
    third_cat_id         INT         COMMENT '三级类目ID',
    brand_id             BIGINT      COMMENT '品牌ID',
    sale_qty             BIGINT      COMMENT '销量',
    sale_amount          DECIMAL(14,2) COMMENT '销售额',
    pay_user_count       BIGINT      COMMENT '支付买家数',
    uv_count             BIGINT      COMMENT '访客数',
    pay_rate             DECIMAL(10,4) COMMENT '支付转化率',
    PRIMARY KEY (dt, product_id),
    INDEX idx_cat1 (first_cat_id),
    INDEX idx_brand (brand_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS: 商品日销售汇总';
    
-- 2.商品日流量来源
DROP TABLE IF EXISTS dws_goods_traffic_source_1d;
CREATE TABLE dws_goods_traffic_source_1d (
    dt                   DATE        NOT NULL COMMENT '业务日期',
    product_id           BIGINT      NOT NULL COMMENT '商品ID',
    source_type          VARCHAR(50) NOT NULL COMMENT '流量来源编码',
    source_name          VARCHAR(100) COMMENT '流量来源名称',
    uv_count             BIGINT      COMMENT '访客数',
    pay_user_count       BIGINT      COMMENT '支付买家数',
    PRIMARY KEY (dt, product_id, source_type),
    INDEX idx_src (source_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS: 商品日流量来源明细';

-- 3.商品日搜索词汇总
DROP TABLE IF EXISTS dws_goods_search_keyword_1d;
CREATE TABLE dws_goods_search_keyword_1d (
                                             dt DATE COMMENT '日期',
                                             product_id BIGINT COMMENT '商品ID',
                                             search_keyword VARCHAR(200) COMMENT '搜索关键词',
                                             search_count INT COMMENT '搜索次数',
                                             click_count INT COMMENT '点击次数',
                                             click_rate DECIMAL(6,4) COMMENT '点击率'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS: 商品搜索关键词分析宽表';

-- 4.商品 SKU 日销量排行
DROP TABLE IF EXISTS dws_sku_sale_topn_1d;
CREATE TABLE dws_sku_sale_topn_1d (
       dt                   DATE        NOT NULL COMMENT '业务日期',
       product_id           BIGINT      NOT NULL COMMENT '商品ID',
       sku_id               BIGINT      NOT NULL COMMENT 'SKU ID',
       sale_qty             BIGINT      COMMENT '销量',
       sale_amount          DECIMAL(14,2) COMMENT '销售额',
       PRIMARY KEY (dt, product_id, sku_id),
       INDEX idx_sku (sku_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS: 商品SKU日销量汇总';


DROP TABLE IF EXISTS dws_product_daily;
CREATE TABLE dws_product_daily (
                                   product_id        BIGINT       NOT NULL COMMENT '商品ID',
                                   dt                DATE         NOT NULL COMMENT '统计日期',
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
                                   PRIMARY KEY (product_id, dt),
                                   INDEX idx_dt (dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS: 商品日粒度汇总表';

DROP TABLE IF EXISTS dws_traffic_daily;
CREATE TABLE dws_traffic_daily (
                                   product_id    BIGINT       NOT NULL COMMENT '商品ID',
                                   dt            DATE         NOT NULL COMMENT '统计日期',
                                   source_type   VARCHAR(50)  NOT NULL COMMENT '流量来源渠道',
                                   visit_cnt     BIGINT       NOT NULL DEFAULT 0 COMMENT '访客数',
                                   pay_user_cnt  BIGINT       NOT NULL DEFAULT 0 COMMENT '支付买家数',
                                   PRIMARY KEY (product_id, dt, source_type),
                                   INDEX idx_dt (dt),
                                   INDEX idx_source (source_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS: 商品流量来源日粒度汇总表';

TRUNCATE TABLE dws_traffic_daily;
INSERT INTO dws_traffic_daily
SELECT
    v.product_id,
    v.dt,
    COALESCE(t.source_type, 'UNKNOWN') AS source_type,
    COUNT(DISTINCT v.user_id) AS visit_cnt,
    COUNT(DISTINCT p.user_id) AS pay_user_cnt
FROM dwd_user_visit_detail v
         LEFT JOIN (
    SELECT DISTINCT session_id, dt, source_type
    FROM dwd_traffic_source_detail
    WHERE dt >= CURDATE() - INTERVAL 30 DAY AND dt < CURDATE()
) t ON v.session_id = t.session_id AND v.dt = t.dt
         LEFT JOIN dwd_trade_order_detail p
                   ON v.product_id = p.product_id AND p.dt = v.dt
WHERE v.dt >= CURDATE() - INTERVAL 30 DAY AND v.dt < CURDATE()
GROUP BY v.product_id, v.dt, COALESCE(t.source_type, 'UNKNOWN');


-- 7.商品SKU销售分析表 
DROP TABLE IF EXISTS dws_sku_daily;
CREATE TABLE dws_sku_daily (
                               product_id    BIGINT       NOT NULL COMMENT '商品ID',
                               sku_id        BIGINT       NOT NULL COMMENT 'SKU ID',
                               dt            DATE         NOT NULL COMMENT '统计日期',
                               pay_cnt       BIGINT       NOT NULL DEFAULT 0 COMMENT '支付件数',
                               pay_amount    DECIMAL(10,2) NOT NULL DEFAULT 0.0 COMMENT '支付金额',
                               PRIMARY KEY (product_id, sku_id, dt),
                               INDEX idx_dt (dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS: SKU销售日粒度汇总表';


-- 8.商品搜索词分析表 
DROP TABLE IF EXISTS dws_search_daily;
CREATE TABLE dws_search_daily (
                                  product_id     BIGINT       NOT NULL COMMENT '商品ID',
                                  dt             DATE         NOT NULL COMMENT '统计日期',
                                  search_keyword VARCHAR(200) NOT NULL COMMENT '搜索词',
                                  visit_cnt      BIGINT       NOT NULL DEFAULT 0 COMMENT '访客数',
                                  click_cnt      BIGINT       NOT NULL DEFAULT 0 COMMENT '点击数',
                                  pay_user_cnt   BIGINT       NOT NULL DEFAULT 0 COMMENT '支付买家数',
                                  PRIMARY KEY (product_id, dt, search_keyword),
                                  INDEX idx_dt (dt),
                                  INDEX idx_keyword (search_keyword)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS: 商品搜索词日粒度汇总表';

TRUNCATE TABLE dws_search_daily;
INSERT INTO dws_search_daily
SELECT
    k.clicked_product_id AS product_id,
    k.dt,
    k.search_keyword,
    COUNT(DISTINCT k.user_id) AS visit_cnt,
    COUNT(k.log_id) AS click_cnt,
    COUNT(DISTINCT p.user_id) AS pay_user_cnt
FROM dwd_search_keyword_detail k
         LEFT JOIN dwd_trade_order_detail p
                   ON k.clicked_product_id = p.product_id
                       AND p.dt = k.dt
WHERE k.dt >= CURDATE() - INTERVAL 30 DAY
  AND k.dt < CURDATE()
  AND k.clicked_product_id IS NOT NULL
GROUP BY k.clicked_product_id, k.dt, k.search_keyword;


DROP TABLE IF EXISTS dws_price_force_daily;
CREATE TABLE dws_price_force_daily (
                                       product_id        BIGINT       NOT NULL COMMENT '商品ID',
                                       dt                DATE         NOT NULL COMMENT '统计日期',
                                       price_force_level TINYINT      NOT NULL COMMENT '价格力等级(1-5星)',
                                       current_price     DECIMAL(10,2) NOT NULL DEFAULT 0.0 COMMENT '当前价格',
                                       market_price      DECIMAL(10,2) NOT NULL DEFAULT 0.0 COMMENT '市场均价',
                                       pay_conversion    DECIMAL(5,2) NOT NULL DEFAULT 0.0 COMMENT '支付转化率',
                                       pay_amount        DECIMAL(10,2) NOT NULL DEFAULT 0.0 COMMENT '支付金额',
                                       pay_cnt           BIGINT       NOT NULL DEFAULT 0 COMMENT '支付件数',
                                       PRIMARY KEY (product_id, dt),
                                       INDEX idx_dt (dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS: 价格力商品日粒度汇总表';


DROP TABLE IF EXISTS temp_current_price;
CREATE TABLE temp_current_price AS
SELECT
    pc1.product_id,
    pc1.new_price,
    pi.third_cat_id
FROM dwd_price_change_detail pc1
         INNER JOIN (
    SELECT
        product_id,
        MAX(dt) AS max_dt
    FROM dwd_price_change_detail
    GROUP BY product_id
) pc2 ON pc1.product_id = pc2.product_id AND pc1.dt = pc2.max_dt
         JOIN ods_product_info pi ON pc1.product_id = pi.product_id;

TRUNCATE TABLE dws_price_force_daily;
INSERT INTO dws_price_force_daily
SELECT
    p.product_id,
    p.dt,
    CASE
        WHEN pr.price_rank <= 0.2 THEN 5
        WHEN pr.price_rank <= 0.4 THEN 4
        WHEN pr.price_rank <= 0.6 THEN 3
        WHEN pr.price_rank <= 0.8 THEN 2
        ELSE 1
        END AS price_force_level,
    COALESCE(MAX(pc.new_price), MAX(p.price), 0.0) AS current_price,
    COALESCE(MAX(cat_avg.avg_price), 0.0) AS market_price,
    COALESCE((COUNT(DISTINCT p.user_id) / NULLIF(COUNT(DISTINCT v.user_id), 0)) * 100, 0) AS pay_conversion,
    SUM(p.amount) AS pay_amount,
    COUNT(p.item_id) AS pay_cnt
FROM dwd_trade_order_detail p
         LEFT JOIN dwd_user_visit_detail v
                   ON p.product_id = v.product_id
                       AND v.dt = p.dt
         LEFT JOIN (
    SELECT
        pc1.product_id,
        pc1.new_price
    FROM dwd_price_change_detail pc1
             INNER JOIN (
        SELECT
            product_id,
            MAX(dt) AS max_dt
        FROM dwd_price_change_detail
        GROUP BY product_id
    ) pc2 ON pc1.product_id = pc2.product_id AND pc1.dt = pc2.max_dt
) pc ON p.product_id = pc.product_id
         LEFT JOIN ods_product_info cat ON p.product_id = cat.product_id
         LEFT JOIN (
    SELECT
        third_cat_id,
        AVG(new_price) AS avg_price
    FROM temp_current_price
    GROUP BY third_cat_id
) cat_avg ON cat.third_cat_id = cat_avg.third_cat_id
         LEFT JOIN (
    SELECT
        c1.product_id,
        c1.third_cat_id,
        c1.new_price,
        COALESCE(
                (SELECT COUNT(*)
                 FROM temp_current_price c2
                 WHERE c2.third_cat_id = c1.third_cat_id
                   AND c2.new_price <= c1.new_price
                ) / (
                    SELECT COUNT(*)
                    FROM temp_current_price c3
                    WHERE c3.third_cat_id = c1.third_cat_id
                ),
                0
        ) AS price_rank
    FROM temp_current_price c1
) pr ON p.product_id = pr.product_id
WHERE p.dt >= CURDATE() - INTERVAL 30 DAY
  AND p.dt < CURDATE()
GROUP BY p.product_id, p.dt, pr.price_rank;



TRUNCATE TABLE dws_sku_daily;
INSERT INTO dws_sku_daily
SELECT
    p.product_id,
    p.sku_id,
    p.dt,
    COUNT(p.item_id) AS pay_cnt,
    SUM(p.amount) AS pay_amount
FROM dwd_trade_order_detail p
WHERE p.dt >= CURDATE() - INTERVAL 30 DAY
  AND p.dt < CURDATE()
GROUP BY p.product_id, p.sku_id, p.dt;

TRUNCATE TABLE dws_product_daily;
INSERT INTO dws_product_daily
SELECT
    prod.product_id,
    date_dim.dt,
    COALESCE(v.visit_cnt, 0) AS visit_cnt,
    COALESCE(m.micro_visit_cnt, 0) AS micro_visit_cnt,
    COALESCE(v.browse_cnt, 0) AS browse_cnt,
    COALESCE(s.avg_stay_duration, 0) AS avg_stay_duration,
    COALESCE(s.bounce_rate, 0) AS bounce_rate,
    COALESCE(c.cart_add_cnt, 0) AS cart_add_cnt,
    COALESCE(o.order_user_cnt, 0) AS order_user_cnt,
    COALESCE(o.order_cnt, 0) AS order_cnt,
    COALESCE(o.order_amount, 0) AS order_amount,
    COALESCE(p.pay_user_cnt, 0) AS pay_user_cnt,
    COALESCE(p.pay_cnt, 0) AS pay_cnt,
    COALESCE(p.pay_amount, 0) AS pay_amount
FROM (
         SELECT DISTINCT product_id
         FROM dwd_trade_order_detail
         UNION
         SELECT DISTINCT product_id
         FROM dwd_user_visit_detail
         UNION
         SELECT DISTINCT product_id
         FROM dwd_cart_action_detail
     ) prod
         CROSS JOIN (
    SELECT DISTINCT dt
    FROM (
             SELECT dt FROM dwd_user_visit_detail
             UNION
             SELECT dt FROM dwd_trade_order_detail
             UNION
             SELECT dt FROM dwd_cart_action_detail
         ) dates
    WHERE dt >= CURDATE() - INTERVAL 30 DAY
      AND dt < CURDATE()
) date_dim
         LEFT JOIN (
    SELECT
        product_id,
        dt,
        COUNT(DISTINCT user_id) AS visit_cnt,
        COUNT(log_id) AS browse_cnt
    FROM dwd_user_visit_detail
    WHERE dt >= CURDATE() - INTERVAL 30 DAY
      AND dt < CURDATE()
    GROUP BY product_id, dt
) v ON prod.product_id = v.product_id AND date_dim.dt = v.dt
         LEFT JOIN (
    SELECT
        product_id,
        DATE(event_time) AS dt,
        COUNT(DISTINCT user_id) AS micro_visit_cnt
    FROM ods_micro_detail_visit_log
    WHERE event_time >= CURDATE() - INTERVAL 30 DAY
      AND event_time < CURDATE()
    GROUP BY product_id, DATE(event_time)
) m ON prod.product_id = m.product_id AND date_dim.dt = m.dt
         LEFT JOIN (
    SELECT
        product_id,
        DATE(entry_time) AS dt,
        AVG(stay_seconds) AS avg_stay_duration,
        (SUM(is_bounce) / COUNT(*)) * 100 AS bounce_rate
    FROM ods_page_stay_log
    WHERE page_type = 'PRODUCT_DETAIL'
      AND entry_time >= CURDATE() - INTERVAL 30 DAY
      AND entry_time < CURDATE()
    GROUP BY product_id, DATE(entry_time)
) s ON prod.product_id = s.product_id AND date_dim.dt = s.dt
         LEFT JOIN (
    SELECT
        product_id,
        dt,
        COUNT(DISTINCT user_id) AS cart_add_cnt
    FROM dwd_cart_action_detail
    WHERE dt >= CURDATE() - INTERVAL 30 DAY
      AND dt < CURDATE()
      AND action_type = 'ADD'
    GROUP BY product_id, dt
) c ON prod.product_id = c.product_id AND date_dim.dt = c.dt
         LEFT JOIN (
    SELECT
        product_id,
        dt,
        COUNT(DISTINCT user_id) AS order_user_cnt,
        COUNT(item_id) AS order_cnt,
        SUM(amount) AS order_amount
    FROM (
             SELECT
                 oi.product_id,
                 oh.user_id,
                 oi.item_id,
                 oi.amount,
                 DATE(oh.order_time) AS dt
             FROM ods_order_header oh
                      JOIN ods_order_item oi ON oh.order_id = oi.order_id
             WHERE oh.order_time >= CURDATE() - INTERVAL 30 DAY
               AND oh.order_time < CURDATE()
         ) t
    GROUP BY product_id, dt
) o ON prod.product_id = o.product_id AND date_dim.dt = o.dt
         LEFT JOIN (
    SELECT
        product_id,
        dt,
        COUNT(DISTINCT user_id) AS pay_user_cnt,
        COUNT(item_id) AS pay_cnt,
        SUM(amount) AS pay_amount
    FROM dwd_trade_order_detail
    WHERE dt >= CURDATE() - INTERVAL 30 DAY
      AND dt < CURDATE()
    GROUP BY product_id, dt
) p ON prod.product_id = p.product_id AND date_dim.dt = p.dt
WHERE COALESCE(v.visit_cnt, 0) + COALESCE(m.micro_visit_cnt, 0) + COALESCE(s.avg_stay_duration, 0) +
      COALESCE(c.cart_add_cnt, 0) + COALESCE(o.order_user_cnt, 0) + COALESCE(p.pay_user_cnt, 0) > 0;



INSERT INTO dws_goods_sale_1d
SELECT
    t.dt,
    t.product_id,
    p.first_cat_id,
    p.second_cat_id,
    p.third_cat_id,
    p.brand_id,
    SUM(t.quantity)             AS sale_qty,
    SUM(t.amount)               AS sale_amount,
    COUNT(DISTINCT t.user_id)   AS pay_user_count,
    uv.uv_count                 AS uv_count,
    ROUND(COUNT(DISTINCT t.user_id) / uv.uv_count, 4) AS pay_rate
FROM dwd_trade_order_detail t
         LEFT JOIN (
    SELECT dt, product_id, COUNT(DISTINCT user_id) AS uv_count
    FROM dwd_user_visit_detail
    GROUP BY dt, product_id
) uv ON t.dt = uv.dt AND t.product_id = uv.product_id
         JOIN dim_product p ON t.product_id = p.product_id
GROUP BY t.dt, t.product_id, p.first_cat_id, p.second_cat_id, p.third_cat_id, p.brand_id;

INSERT INTO dws_goods_traffic_source_1d
SELECT
    sk.dt,
    sk.product_id,
    sk.source_type,
    sk.source_name,
    COALESCE(stats.uv_count, 0)       AS uv_count,
    COALESCE(stats.pay_user_count, 0) AS pay_user_count
FROM (
         SELECT
             p.dt,
             p.product_id,
             c.source_type,
             c.source_name
         FROM
             (SELECT DISTINCT dt, product_id FROM dwd_user_visit_detail) AS p
                 CROSS JOIN
             dim_traffic_source AS c
     ) AS sk
         LEFT JOIN (
    SELECT
        ts.dt,
        uv.product_id,
        ts.source_type,
        COUNT(DISTINCT uv.user_id)   AS uv_count,
        COUNT(DISTINCT t.user_id)    AS pay_user_count
    FROM dwd_traffic_source_detail ts
             JOIN dwd_user_visit_detail uv
                  ON ts.user_id = uv.user_id
                      AND ts.dt      = uv.dt
             LEFT JOIN dwd_trade_order_detail t
                       ON uv.dt         = t.dt
                           AND uv.product_id = t.product_id
                           AND uv.user_id    = t.user_id
    GROUP BY ts.dt, uv.product_id, ts.source_type
) AS stats
                   ON sk.dt          = stats.dt
                       AND sk.product_id  = stats.product_id
                       AND sk.source_type = stats.source_type;
INSERT INTO dws_goods_search_keyword_1d
SELECT
    s.dt,
    s.clicked_product_id      AS product_id,
    s.search_keyword,
    COUNT(*)                  AS search_count,
    SUM(CASE WHEN s.clicked_product_id IS NOT NULL THEN 1 ELSE 0 END) AS click_count,
    ROUND(
            IF(COUNT(*) = 0, 0, SUM(CASE WHEN s.clicked_product_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*)),
            4
    ) AS click_rate
FROM dwd_search_keyword_detail s
GROUP BY s.dt, s.clicked_product_id, s.search_keyword;

INSERT INTO dws_sku_sale_topn_1d
SELECT
    t.dt,
    t.product_id,
    t.sku_id,
    SUM(t.quantity)   AS sale_qty,
    SUM(t.amount)     AS sale_amount
FROM dwd_trade_order_detail t
GROUP BY t.dt, t.product_id, t.sku_id;
