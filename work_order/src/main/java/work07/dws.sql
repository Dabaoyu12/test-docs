-- ====================================================================
-- DWS层表创建脚本
-- 工单编号：大数据-电商数仓-07-商品主题商品诊断看板
-- ====================================================================

USE gmall_work_07;

-- 1. 商品诊断宽表（不分层）
CREATE TABLE IF NOT EXISTS dws_product_diagnosis (
    product_id BIGINT NOT NULL COMMENT '商品ID',
    date_key DATE NOT NULL COMMENT '日期',
    product_name VARCHAR(255) NOT NULL COMMENT '商品名称',
    category_name VARCHAR(100) NOT NULL COMMENT '类目名称',
    brand_name VARCHAR(100) NOT NULL COMMENT '品牌名称',

    -- 五维指标得分
    traffic_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '流量获取得分',
    conversion_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '转化行为得分',
    engagement_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '内容营销得分',
    acquisition_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '客户拉新得分',
    service_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '服务质量得分',
    total_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '综合得分',
    grade CHAR(1) NOT NULL COMMENT '评级(A/B/C/D)',

    -- 竞品对比
    market_avg_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '竞品平均分',
    traffic_score_diff DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '流量得分差值',
    conversion_score_diff DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '转化得分差值',

    -- 关键二级指标
    uv_count BIGINT NOT NULL DEFAULT 0 COMMENT '总访客数',
    pay_conversion_rate DECIMAL(5,4) NOT NULL DEFAULT 0 COMMENT '支付转化率',
    cart_conversion_rate DECIMAL(5,4) NOT NULL DEFAULT 0 COMMENT '加购转化率',
    new_buyer_ratio DECIMAL(5,4) NOT NULL DEFAULT 0 COMMENT '新买家占比',
    refund_rate DECIMAL(5,4) NOT NULL DEFAULT 0 COMMENT '退款率',
    pic_review_count BIGINT NOT NULL DEFAULT 0 COMMENT '有图评价数',
PRIMARY KEY (product_id, date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品诊断宽表（不分层）';

-- 2. 商品等级诊断宽表（分层）
CREATE TABLE IF NOT EXISTS dws_product_grade_diagnosis (
    grade CHAR(1) NOT NULL COMMENT '商品评级(A/B/C/D)',
    date_key DATE NOT NULL COMMENT '日期',

    -- 五维指标平均分
    avg_traffic_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '平均流量得分',
    avg_conversion_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '平均转化得分',
    avg_engagement_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '平均内容得分',
    avg_acquisition_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '平均拉新得分',
    avg_service_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '平均服务得分',
    avg_total_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '平均综合得分',

    -- 分布统计
    product_count INT NOT NULL DEFAULT 0 COMMENT '商品数量',
    top_product_id BIGINT NULL COMMENT 'TOP商品ID',
    top_product_name VARCHAR(255) NULL COMMENT 'TOP商品名称',
    top_score DECIMAL(5,2) NULL COMMENT 'TOP商品得分',

    -- 竞品对比
    market_avg_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '竞品平均分',
    avg_score_diff DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '平均分差值',

                                                           PRIMARY KEY (grade, date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品等级诊断宽表（分层）';


-- 1. 填充商品诊断宽表（不分层）
INSERT INTO dws_product_diagnosis (
    product_id, date_key, product_name, category_name, brand_name,
    traffic_score, conversion_score, engagement_score,
    acquisition_score, service_score, total_score, grade,
    market_avg_score, traffic_score_diff, conversion_score_diff,
    uv_count, pay_conversion_rate, cart_conversion_rate,
    new_buyer_ratio, refund_rate, pic_review_count
)
SELECT
    e.product_id,
    e.date_key,
    p.product_name,
    p.category_name,
    p.brand_name,
    e.traffic_score,
    e.conversion_score,
    e.engagement_score,
    e.acquisition_score,
    e.service_score,
    e.total_score,
    e.grade,
    e.market_avg_score,
    -- 计算与竞品的差值
    e.traffic_score - e.market_avg_score AS traffic_score_diff,
    e.conversion_score - e.market_avg_score AS conversion_score_diff,
    -- 关键二级指标
    t.total_uv AS uv_count,
    c.pay_conversion_rate,
    c.cart_conversion_rate,
    a.new_buyer_ratio,
    a.refund_rate,
    s.pic_review_count
FROM dwd_product_evaluation e
         JOIN ods_dim_product p ON e.product_id = p.product_id
         JOIN dwd_product_traffic t ON e.product_id = t.product_id AND e.date_key = t.date_key
         JOIN dwd_product_conversion c ON e.product_id = c.product_id AND e.date_key = c.date_key
         JOIN dwd_product_acquisition a ON e.product_id = a.product_id AND e.date_key = a.date_key
         JOIN dwd_product_service s ON e.product_id = s.product_id AND e.date_key = s.date_key
WHERE e.date_key = CURRENT_DATE; -- 按天增量更新

-- 2. 填充商品等级诊断宽表（分层）

TRUNCATE TABLE dws_product_grade_diagnosis;
-- 创建临时表存储排序数据
CREATE TEMPORARY TABLE tmp_ranked_data (
    product_id BIGINT,
    date_key DATE,
    grade CHAR(1),
    traffic_score DECIMAL(5,2),
    conversion_score DECIMAL(5,2),
    engagement_score DECIMAL(5,2),
    acquisition_score DECIMAL(5,2),
    service_score DECIMAL(5,2),
    total_score DECIMAL(5,2),
    market_avg_score DECIMAL(5,2),
    product_name VARCHAR(255),
    rk INT
);

-- 使用用户变量生成排名
SET @current_grade = '';
SET @current_date = '';
SET @rank = 0;

INSERT INTO tmp_ranked_data
SELECT
    product_id,
    date_key,
    grade,
    traffic_score,
    conversion_score,
    engagement_score,
    acquisition_score,
    service_score,
    total_score,
    market_avg_score,
    product_name,
    rk
FROM (
         SELECT
             e.product_id,
             e.date_key,
             e.grade,
             e.traffic_score,
             e.conversion_score,
             e.engagement_score,
             e.acquisition_score,
             e.service_score,
             e.total_score,
             e.market_avg_score,
             p.product_name,
             -- 使用用户变量生成排名
             @rank := IF(@current_grade = e.grade AND @current_date = e.date_key, @rank + 1, 1) AS rk,
             @current_grade := e.grade,
             @current_date := e.date_key
         FROM dwd_product_evaluation e
                  JOIN ods_dim_product p ON e.product_id = p.product_id
         WHERE e.date_key BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY) AND CURRENT_DATE
         ORDER BY e.grade, e.date_key, e.total_score DESC
     ) ranked_data;

-- 创建临时表存储TOP商品信息 - 确保唯一性
CREATE TEMPORARY TABLE tmp_top_products AS
SELECT
    grade,
    date_key,
    MAX(product_id) AS top_product_id,  -- 如果有多个，取最大ID
    MAX(product_name) AS top_product_name, -- 如果有多个，取最大名称
    MAX(total_score) AS top_score
FROM tmp_ranked_data
WHERE rk = 1
GROUP BY grade, date_key;  -- 确保每个等级和日期只有一行

-- 创建临时表存储聚合结果
CREATE TEMPORARY TABLE tmp_aggregated AS
SELECT
    grade,
    date_key,
    AVG(traffic_score) AS avg_traffic_score,
    AVG(conversion_score) AS avg_conversion_score,
    AVG(engagement_score) AS avg_engagement_score,
    AVG(acquisition_score) AS avg_acquisition_score,
    AVG(service_score) AS avg_service_score,
    AVG(total_score) AS avg_total_score,
    COUNT(DISTINCT product_id) AS product_count,
    AVG(market_avg_score) AS market_avg_score,
    AVG(total_score) - AVG(market_avg_score) AS avg_score_diff
FROM tmp_ranked_data
GROUP BY grade, date_key;

-- 插入最终数据 - 使用内连接确保唯一性
INSERT INTO dws_product_grade_diagnosis (
    grade, date_key,
    avg_traffic_score, avg_conversion_score, avg_engagement_score,
    avg_acquisition_score, avg_service_score, avg_total_score,
    product_count, top_product_id, top_product_name, top_score,
    market_avg_score, avg_score_diff
)
SELECT
    a.grade,
    a.date_key,
    a.avg_traffic_score,
    a.avg_conversion_score,
    a.avg_engagement_score,
    a.avg_acquisition_score,
    a.avg_service_score,
    a.avg_total_score,
    a.product_count,
    t.top_product_id,
    t.top_product_name,
    t.top_score,
    a.market_avg_score,
    a.avg_score_diff
FROM tmp_aggregated a
         JOIN tmp_top_products t
              ON a.grade = t.grade
                  AND a.date_key = t.date_key;

-- 清理临时表
DROP TEMPORARY TABLE tmp_ranked_data;
DROP TEMPORARY TABLE tmp_top_products;
DROP TEMPORARY TABLE tmp_aggregated;