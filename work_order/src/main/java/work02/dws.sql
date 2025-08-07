USE gmall_work_02;

-- 1. dws_goods_sale_1d：商品日销售概览
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
    
-- 2. dws_goods_traffic_source_1d：商品日流量来源
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

-- 3. dws_goods_search_keyword_1d：商品日搜索词汇总
DROP TABLE IF EXISTS dws_goods_search_keyword_1d;
CREATE TABLE dws_goods_search_keyword_1d (
                                             dt DATE COMMENT '日期',
                                             product_id BIGINT COMMENT '商品ID',
                                             search_keyword VARCHAR(200) COMMENT '搜索关键词',
                                             search_count INT COMMENT '搜索次数',
                                             click_count INT COMMENT '点击次数',
                                             click_rate DECIMAL(6,4) COMMENT '点击率'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS: 商品搜索关键词分析宽表';

-- 4. dws_sku_sale_topn_1d：商品 SKU 日销量排行（在 ADS 层再做 TOPN）
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

-- 5. dws_price_power_dist_1d：商品价格力分布（日）
DROP TABLE IF EXISTS dws_price_power_dist_1d;
CREATE TABLE dws_price_power_dist_1d (
          dt                   DATE        NOT NULL COMMENT '业务日期',
          product_id           BIGINT      NOT NULL COMMENT '商品ID',
          price_star           TINYINT     COMMENT '价格力星级',
          PRIMARY KEY (dt, product_id),
          INDEX idx_star (price_star)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS: 商品日价格力分布';

-- 6. dws_price_power_warning_1d：价格力预警商品
DROP TABLE IF EXISTS dws_price_power_warning_1d;
CREATE TABLE dws_price_power_warning_1d (
             dt                   DATE        NOT NULL COMMENT '业务日期',
             product_id           BIGINT      NOT NULL COMMENT '商品ID',
             warning_type         VARCHAR(50) COMMENT '预警类型',
             metric_value         DECIMAL(14,2) COMMENT '预警指标值',
             PRIMARY KEY (dt, product_id, warning_type),
             INDEX idx_warn_type (warning_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS: 商品价格力预警汇总';



-- 1. 填充 dws_goods_sale_1d
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

-- 2. 填充 dws_goods_traffic_source_1d
INSERT INTO dws_goods_traffic_source_1d
SELECT
    sk.dt,
    sk.product_id,
    sk.source_type,
    sk.source_name,
    COALESCE(stats.uv_count, 0)       AS uv_count,
    COALESCE(stats.pay_user_count, 0) AS pay_user_count
FROM (
         -- 修正后的骨架
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
    -- 实际统计
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
-- 3. 填充 dws_goods_search_keyword_1d
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

-- 4. 填充 dws_sku_sale_topn_1d
INSERT INTO dws_sku_sale_topn_1d
SELECT
    t.dt,
    t.product_id,
    t.sku_id,
    SUM(t.quantity)   AS sale_qty,
    SUM(t.amount)     AS sale_amount
FROM dwd_trade_order_detail t
GROUP BY t.dt, t.product_id, t.sku_id;

-- 5. 填充 dws_price_power_dist_1d

INSERT INTO dws_price_power_dist_1d
SELECT
    c.dt,
    c.product_id,
    AVG(c.price_star) AS price_star  -- 例如：计算当日该商品的平均评分
-- 或 MAX(c.price_star) 取最高评分，根据业务需求选择
FROM dwd_price_change_detail c
GROUP BY c.dt, c.product_id;

-- 6. 填充 dws_price_power_warning_1d
-- 6. 填充 dws_price_power_warning_1d
INSERT INTO dws_price_power_warning_1d (dt, product_id, warning_type, metric_value)
SELECT
    dt,
    product_id,
    'PRICE_RISE_10P' AS warning_type,
    ROUND((max_price - min_price) / min_price * 100, 2) AS metric_value
FROM (
         SELECT
             dt,
             product_id,
             MIN(new_price) AS min_price,
             MAX(new_price) AS max_price
         FROM dwd_price_change_detail
         GROUP BY dt, product_id
     ) t
WHERE (max_price - min_price) / min_price > 0.1  -- 涨幅阈值10%

UNION ALL

SELECT
    dt,
    product_id,
    'PRICE_DROP_10P' AS warning_type,
    ROUND((max_price - min_price) / max_price * 100, 2) AS metric_value
FROM (
         SELECT
             dt,
             product_id,
             MIN(new_price) AS min_price,
             MAX(new_price) AS max_price
         FROM dwd_price_change_detail
         GROUP BY dt, product_id
     ) t
WHERE (max_price - min_price) / max_price > 0.1;  -- 跌幅阈值10%
