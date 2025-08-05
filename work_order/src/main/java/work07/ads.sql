-- ====================================================================
-- ADS层表创建脚本
-- 工单编号：大数据-电商数仓-07-商品主题商品诊断看板
-- ====================================================================

USE gmall_work_07;
CREATE TABLE IF NOT EXISTS ads_product_overview (
    date_key DATE NOT NULL COMMENT '日期',
    analysis_dimension ENUM('score','price','visitor') NOT NULL COMMENT '分析维度：score-评分维度，price-价格维度，visitor-访客维度',
    grade CHAR(1) NOT NULL COMMENT '商品评级(A/B/C/D，评分维度有效，其他维度可设为特定值如空字符或"0")',
    product_count BIGINT NOT NULL DEFAULT 0 COMMENT '商品数量',
    product_ratio DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '商品占比(%)',
    total_amount DECIMAL(15,2) NOT NULL DEFAULT 0 COMMENT '总金额(价格维度有效)',
    avg_price DECIMAL(10,2) NOT NULL DEFAULT 0 COMMENT '平均价格(价格维度有效)',
    total_sales BIGINT NOT NULL DEFAULT 0 COMMENT '总销量',
    total_uv BIGINT NOT NULL DEFAULT 0 COMMENT '总访客数(访客维度有效)',
    uv_sales_ratio DECIMAL(10,4) NOT NULL DEFAULT 0 COMMENT '访客-销量比(访客维度有效)',
    avg_total_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '平均综合得分(评分维度有效)',
    PRIMARY KEY (date_key, analysis_dimension, grade)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品全品价值评估总览表';



INSERT INTO ads_product_overview (
    date_key, analysis_dimension, grade,
    product_count, product_ratio, total_amount, avg_price,
    total_sales, total_uv, uv_sales_ratio, avg_total_score
)
-- 1. 评分维度数据（analysis_dimension='score'）
SELECT
    d.date_key,
    'score' AS analysis_dimension,
    d.grade,
    COUNT(DISTINCT d.product_id) AS product_count,
    ROUND(COUNT(DISTINCT d.product_id) / (SELECT COUNT(DISTINCT product_id) FROM dws_product_diagnosis WHERE date_key = d.date_key) * 100, 2) AS product_ratio,
    0 AS total_amount,
    0 AS avg_price,
    0 AS total_sales,
    SUM(d.uv_count) AS total_uv,
    0 AS uv_sales_ratio,
    AVG(d.total_score) AS avg_total_score
FROM dws_product_diagnosis d
GROUP BY d.date_key, d.grade

UNION ALL

-- 2. 价格维度数据（analysis_dimension='price'）
SELECT
    p.date_key,
    'price' AS analysis_dimension,
    '' AS grade,
    COUNT(DISTINCT p.product_id) AS product_count,
    0 AS product_ratio,
    SUM(s.subtotal_amount) AS total_amount,
    AVG(sku.price) AS avg_price,
    SUM(s.quantity) AS total_sales,
    0 AS total_uv,
    0 AS uv_sales_ratio,
    0 AS avg_total_score
FROM dws_product_diagnosis p
         JOIN ods_fact_order_item s ON p.product_id = s.product_id
         JOIN ods_dim_sku sku ON s.sku_id = sku.sku_id
GROUP BY p.date_key

UNION ALL

-- 3. 访客维度数据（analysis_dimension='visitor'）
SELECT
    d.date_key,
    'visitor' AS analysis_dimension,
    '' AS grade,
    COUNT(DISTINCT d.product_id) AS product_count,
    0 AS product_ratio,
    0 AS total_amount,
    0 AS avg_price,
    SUM(s.quantity) AS total_sales,
    SUM(d.uv_count) AS total_uv,
    ROUND(SUM(s.quantity) / SUM(d.uv_count), 4) AS uv_sales_ratio,
    0 AS avg_total_score
FROM dws_product_diagnosis d
         JOIN ods_fact_order_item s ON d.product_id = s.product_id
GROUP BY d.date_key;



CREATE TABLE IF NOT EXISTS ads_product_competitor (
    date_key DATE NOT NULL COMMENT '日期',
    product_id BIGINT NOT NULL COMMENT '商品ID',
    product_name VARCHAR(255) NOT NULL COMMENT '商品名称',
    category_name VARCHAR(100) NOT NULL COMMENT '类目名称',
    grade CHAR(1) NOT NULL COMMENT '商品评级',

    traffic_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '流量获取得分',
    traffic_rank INT NOT NULL DEFAULT 0 COMMENT '流量得分类目排名',
    conversion_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '转化行为得分',
    conversion_rank INT NOT NULL DEFAULT 0 COMMENT '转化得分类目排名',
    engagement_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '内容营销得分',
    engagement_rank INT NOT NULL DEFAULT 0 COMMENT '内容得分类目排名',
    acquisition_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '客户拉新得分',
    acquisition_rank INT NOT NULL DEFAULT 0 COMMENT '拉新得分类目排名',
    service_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '服务质量得分',
    service_rank INT NOT NULL DEFAULT 0 COMMENT '服务得分类目排名',

    market_avg_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '竞品平均分',
    total_score_diff DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '综合得分与竞品差值',
    PRIMARY KEY (date_key, product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品单品竞争力详情表';


INSERT INTO ads_product_competitor (
    date_key, product_id, product_name, category_name, grade,
    traffic_score, traffic_rank,
    conversion_score, conversion_rank,
    engagement_score, engagement_rank,
    acquisition_score, acquisition_rank,
    service_score, service_rank,
    market_avg_score, total_score_diff
)
SELECT
    d.date_key,
    d.product_id,
    d.product_name,
    d.category_name,
    d.grade,
    d.traffic_score,

    (SELECT COUNT(*) + 1 FROM dws_product_diagnosis
     WHERE date_key = d.date_key AND category_name = d.category_name
       AND traffic_score > d.traffic_score) AS traffic_rank,
    d.conversion_score,

    (SELECT COUNT(*) + 1 FROM dws_product_diagnosis
     WHERE date_key = d.date_key AND category_name = d.category_name
       AND conversion_score > d.conversion_score) AS conversion_rank,
    d.engagement_score,

    (SELECT COUNT(*) + 1 FROM dws_product_diagnosis
     WHERE date_key = d.date_key AND category_name = d.category_name
       AND engagement_score > d.engagement_score) AS engagement_rank,
    d.acquisition_score,

    (SELECT COUNT(*) + 1 FROM dws_product_diagnosis
     WHERE date_key = d.date_key AND category_name = d.category_name
       AND acquisition_score > d.acquisition_score) AS acquisition_rank,
    d.service_score,

    (SELECT COUNT(*) + 1 FROM dws_product_diagnosis
     WHERE date_key = d.date_key AND category_name = d.category_name
       AND service_score > d.service_score) AS service_rank,
    d.market_avg_score,
    d.total_score - d.market_avg_score AS total_score_diff
FROM dws_product_diagnosis d;