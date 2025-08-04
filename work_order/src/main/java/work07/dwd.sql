-- ====================================================================
-- DWD层表创建脚本
-- 工单编号：大数据-电商数仓-07-商品主题商品诊断看板
-- ====================================================================

USE gmall_work_07;

-- 1. 商品流量获取明细表
CREATE TABLE IF NOT EXISTS dwd_product_traffic (
    product_id BIGINT NOT NULL COMMENT '商品ID',
    date_key DATE NOT NULL COMMENT '日期',
    total_uv BIGINT NOT NULL DEFAULT 0 COMMENT '总访客数',
    hand_search_uv BIGINT NOT NULL DEFAULT 0 COMMENT '手淘搜索引导访客数',
    juhuasuan_uv BIGINT NOT NULL DEFAULT 0 COMMENT '聚划算引导访客数',
    direct_car_uv BIGINT NOT NULL DEFAULT 0 COMMENT '直通车引导访客数',
    traffic_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '流量获取得分',
    PRIMARY KEY (product_id, date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品流量获取明细表';

-- 2. 商品转化行为明细表
CREATE TABLE IF NOT EXISTS dwd_product_conversion (
    product_id BIGINT NOT NULL COMMENT '商品ID',
    date_key DATE NOT NULL COMMENT '日期',
    pay_conversion_rate DECIMAL(5,4) NOT NULL DEFAULT 0 COMMENT '访问-支付转化率',
    cart_conversion_rate DECIMAL(5,4) NOT NULL DEFAULT 0 COMMENT '访问-加购转化率',
    uv_value DECIMAL(10,2) NOT NULL DEFAULT 0 COMMENT '访客平均价值',
    collect_conversion_rate DECIMAL(5,4) NOT NULL DEFAULT 0 COMMENT '访问-收藏转化率',
    conversion_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '转化行为得分',
    PRIMARY KEY (product_id, date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品转化行为明细表';

-- 3. 内容互动明细表
CREATE TABLE IF NOT EXISTS dwd_product_engagement (
    product_id BIGINT NOT NULL COMMENT '商品ID',
    date_key DATE NOT NULL COMMENT '日期',
    content_guide_uv BIGINT NOT NULL DEFAULT 0 COMMENT '内容引导访客数',
    content_collect_rate DECIMAL(5,4) NOT NULL DEFAULT 0 COMMENT '内容引导收藏转化率',
    content_cart_rate DECIMAL(5,4) NOT NULL DEFAULT 0 COMMENT '内容引导加购转化率',
    content_pay_amount DECIMAL(12,2) NOT NULL DEFAULT 0 COMMENT '内容引导支付金额',
    content_pay_buyers BIGINT NOT NULL DEFAULT 0 COMMENT '内容引导支付买家数',
    engagement_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '内容营销得分',
    PRIMARY KEY (product_id, date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='内容互动明细表';

-- 4. 客户拉新明细表
CREATE TABLE IF NOT EXISTS dwd_product_acquisition (
    product_id BIGINT NOT NULL COMMENT '商品ID',
    date_key DATE NOT NULL COMMENT '日期',
    new_buyer_ratio DECIMAL(5,4) NOT NULL DEFAULT 0 COMMENT '支付新买家数占比',
    new_buyer_amount_ratio DECIMAL(5,4) NOT NULL DEFAULT 0 COMMENT '支付新买家金额占比',
    refund_rate DECIMAL(5,4) NOT NULL DEFAULT 0 COMMENT '退款率',
    acquisition_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '客户拉新得分',
    PRIMARY KEY (product_id, date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户拉新明细表';

-- 5. 服务质量明细表
CREATE TABLE IF NOT EXISTS dwd_product_service (
    product_id BIGINT NOT NULL COMMENT '商品ID',
    date_key DATE NOT NULL COMMENT '日期',
    pic_review_count BIGINT NOT NULL DEFAULT 0 COMMENT '有图评价数',
    positive_review_count BIGINT NOT NULL DEFAULT 0 COMMENT '正面评价数',
    service_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '服务质量得分',
    PRIMARY KEY (product_id, date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='服务质量明细表';

-- 6. 商品综合评分表
CREATE TABLE IF NOT EXISTS dwd_product_evaluation (
    product_id BIGINT NOT NULL COMMENT '商品ID',
    date_key DATE NOT NULL COMMENT '日期',
    traffic_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '流量获取得分',
    conversion_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '转化行为得分',
    engagement_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '内容营销得分',
    acquisition_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '客户拉新得分',
    service_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '服务质量得分',
    total_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '综合得分',
    grade CHAR(1) NOT NULL COMMENT '评级(A/B/C/D)',
    market_avg_score DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '竞品平均分',
    PRIMARY KEY (product_id, date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品综合评分表（五维模型）';


-- 清空DWD表（保留表结构）
TRUNCATE TABLE dwd_product_traffic;
TRUNCATE TABLE dwd_product_conversion;
TRUNCATE TABLE dwd_product_engagement;
TRUNCATE TABLE dwd_product_acquisition;
TRUNCATE TABLE dwd_product_service;
TRUNCATE TABLE dwd_product_evaluation;

-- 1. 直接生成流量获取数据（35%权重）
-- ====================================================================
-- 修复后的 DWD 层数据生成脚本
-- 工单编号：大数据-电商数仓-07-商品主题商品诊断看板
-- ====================================================================

USE gmall_work_07;

-- 清空 DWD 表
TRUNCATE TABLE dwd_product_traffic;
TRUNCATE TABLE dwd_product_conversion;
TRUNCATE TABLE dwd_product_engagement;
TRUNCATE TABLE dwd_product_acquisition;
TRUNCATE TABLE dwd_product_service;
TRUNCATE TABLE dwd_product_evaluation;

-- 1. 生成流量获取数据（35%权重）
-- ====================================================================
-- 修复后的 DWD 层数据生成脚本
-- 工单编号：大数据-电商数仓-07-商品主题商品诊断看板
-- ====================================================================

USE gmall_work_07;

-- 清空 DWD 表
TRUNCATE TABLE dwd_product_traffic;
TRUNCATE TABLE dwd_product_conversion;
TRUNCATE TABLE dwd_product_engagement;
TRUNCATE TABLE dwd_product_acquisition;
TRUNCATE TABLE dwd_product_service;
TRUNCATE TABLE dwd_product_evaluation;

-- 1. 生成流量获取数据（35%权重）
INSERT INTO dwd_product_traffic (
    product_id, date_key, total_uv, hand_search_uv,
    juhuasuan_uv, direct_car_uv, traffic_score
)
SELECT
    product_id,
    date_key,
    total_uv,
    hand_search_uv,
    juhuasuan_uv,
    direct_car_uv,
    -- 流量得分计算（确保不超过100）
    LEAST(
            (total_uv * 0.00003 +
             hand_search_uv * 0.00002 +
             juhuasuan_uv * 0.00001 +
             direct_car_uv * 0.00001) * 100,
            100
    ) AS traffic_score
FROM (
         SELECT
             product_id,
             date_key,
             FLOOR(1000 + RAND()*5000) AS total_uv,             -- 总UV 1000-6000
             FLOOR(100 + RAND()*4000) AS hand_search_uv,         -- 手淘搜索UV 100-4100
             FLOOR(50 + RAND()*2000) AS juhuasuan_uv,            -- 聚划算UV 50-2050
             FLOOR(30 + RAND()*1500) AS direct_car_uv            -- 直通车UV 30-1530
         FROM (
                  SELECT DISTINCT p.product_id, d.date_key
                  FROM ods_dim_product p
                           CROSS JOIN ods_dim_date d
                  WHERE d.date_key BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) AND CURRENT_DATE
              ) product_dates
     ) uv_data;

-- 2. 生成转化行为数据（30%权重）
INSERT INTO dwd_product_conversion (
    product_id, date_key, pay_conversion_rate, cart_conversion_rate,
    uv_value, collect_conversion_rate, conversion_score
)
SELECT
    product_id,
    date_key,
    pay_conversion_rate,
    cart_conversion_rate,
    uv_value,
    collect_conversion_rate,
    -- 转化得分计算（确保不超过100）
    LEAST(
            (pay_conversion_rate * 12 +
             cart_conversion_rate * 9 +
             uv_value * 0.06 +
             collect_conversion_rate * 3),
            100
    ) AS conversion_score
FROM (
         SELECT
             product_id,
             date_key,
             ROUND(0.01 + RAND()*0.15, 4) AS pay_conversion_rate,    -- 支付转化率 1%-16%
             ROUND(0.05 + RAND()*0.25, 4) AS cart_conversion_rate,   -- 加购转化率 5%-30%
             ROUND(10 + RAND()*90, 2) AS uv_value,                   -- UV价值 10-100元
             ROUND(0.02 + RAND()*0.10, 4) AS collect_conversion_rate -- 收藏转化率 2%-12%
         FROM (
                  SELECT DISTINCT p.product_id, d.date_key
                  FROM ods_dim_product p
                           CROSS JOIN ods_dim_date d
                  WHERE d.date_key BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) AND CURRENT_DATE
              ) product_dates
     ) conversion_data;

-- 3. 修复内容互动数据（10%权重）
INSERT INTO dwd_product_engagement (
    product_id, date_key, content_guide_uv, content_collect_rate,
    content_cart_rate, content_pay_amount, content_pay_buyers, engagement_score
)
SELECT
    product_id,
    date_key,
    content_guide_uv,
    content_collect_rate,
    content_cart_rate,
    content_pay_amount,
    content_pay_buyers,
    -- 修复内容营销得分计算（确保不超过100）
    LEAST(
            (content_guide_uv * 0.03 +
             content_collect_rate * 20 +  -- 转换为百分比
             content_cart_rate * 20 +     -- 转换为百分比
             content_pay_amount * 0.0002 +
             content_pay_buyers * 0.2),
            100
    ) AS engagement_score
FROM (
         SELECT
             product_id,
             date_key,
             FLOOR(50 + RAND()*500) AS content_guide_uv,           -- 内容引导UV 50-550
             ROUND(0.01 + RAND()*0.20, 4) AS content_collect_rate, -- 内容收藏率 1%-21%
             ROUND(0.02 + RAND()*0.15, 4) AS content_cart_rate,    -- 内容加购率 2%-17%
             ROUND(500 + RAND()*5000, 2) AS content_pay_amount,    -- 内容支付金额 500-5500元
             FLOOR(10 + RAND()*100) AS content_pay_buyers          -- 内容支付买家 10-110人
         FROM (
                  SELECT DISTINCT p.product_id, d.date_key
                  FROM ods_dim_product p
                           CROSS JOIN ods_dim_date d
                  WHERE d.date_key BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) AND CURRENT_DATE
              ) product_dates
     ) engagement_data;

-- 4. 生成客户拉新数据（10%权重）
INSERT INTO dwd_product_acquisition (
    product_id, date_key, new_buyer_ratio, new_buyer_amount_ratio,
    refund_rate, acquisition_score
)
SELECT
    product_id,
    date_key,
    new_buyer_ratio,
    new_buyer_amount_ratio,
    refund_rate,
    -- 客户拉新得分（确保不超过100）
    LEAST(
            (new_buyer_ratio * 100 * 0.05 +
             new_buyer_amount_ratio * 100 * 0.05 -
             refund_rate * 100 * 0.03),
            100
    ) AS acquisition_score
FROM (
         SELECT
             product_id,
             date_key,
             ROUND(0.05 + RAND()*0.40, 4) AS new_buyer_ratio,          -- 新买家占比 5%-45%
             ROUND(0.10 + RAND()*0.50, 4) AS new_buyer_amount_ratio,   -- 新买家金额占比 10%-60%
             ROUND(0.01 + RAND()*0.15, 4) AS refund_rate               -- 退款率 1%-16%
         FROM (
                  SELECT DISTINCT p.product_id, d.date_key
                  FROM ods_dim_product p
                           CROSS JOIN ods_dim_date d
                  WHERE d.date_key BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) AND CURRENT_DATE
              ) product_dates
     ) acquisition_data;

-- 5. 生成服务质量数据（15%权重）
INSERT INTO dwd_product_service (
    product_id, date_key, pic_review_count, positive_review_count, service_score
)
SELECT
    product_id,
    date_key,
    pic_review_count,
    positive_review_count,
    -- 服务质量得分（确保不超过100）
    LEAST(
            (pic_review_count * 0.06 +
             positive_review_count * 0.06),
            100
    ) AS service_score
FROM (
         SELECT
             product_id,
             date_key,
             FLOOR(5 + RAND()*100) AS pic_review_count,             -- 有图评价数 5-105
             FLOOR(10 + RAND()*200) AS positive_review_count        -- 正面评价数 10-210
         FROM (
                  SELECT DISTINCT p.product_id, d.date_key
                  FROM ods_dim_product p
                           CROSS JOIN ods_dim_date d
                  WHERE d.date_key BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) AND CURRENT_DATE
              ) product_dates
     ) service_data;

-- 6. 生成综合评分数据（含竞品对比）
INSERT INTO dwd_product_evaluation (
    product_id, date_key, traffic_score, conversion_score,
    engagement_score, acquisition_score, service_score,
    total_score, grade, market_avg_score
)
SELECT
    t.product_id,
    t.date_key,
    t.traffic_score,
    c.conversion_score,
    e.engagement_score,
    a.acquisition_score,
    s.service_score,
    -- 综合得分 = 流量得分(35%) + 转化得分(30%) + 内容得分(10%) + 拉新得分(10%) + 服务得分(15%)
    LEAST(
            (t.traffic_score * 0.35 +
             c.conversion_score * 0.30 +
             e.engagement_score * 0.10 +
             a.acquisition_score * 0.10 +
             s.service_score * 0.15),
            100
    ) AS total_score,
    -- 评级分档逻辑
    CASE
        WHEN (t.traffic_score * 0.35 +
              c.conversion_score * 0.30 +
              e.engagement_score * 0.10 +
              a.acquisition_score * 0.10 +
              s.service_score * 0.15) >= 85 THEN 'A'
        WHEN (t.traffic_score * 0.35 +
              c.conversion_score * 0.30 +
              e.engagement_score * 0.10 +
              a.acquisition_score * 0.10 +
              s.service_score * 0.15) >= 70 THEN 'B'
        WHEN (t.traffic_score * 0.35 +
              c.conversion_score * 0.30 +
              e.engagement_score * 0.10 +
              a.acquisition_score * 0.10 +
              s.service_score * 0.15) >= 50 THEN 'C'
        ELSE 'D'
        END AS grade,
    -- 竞品平均分（随机生成70-90之间的值）
    ROUND(70 + RAND()*20, 2) AS market_avg_score
FROM dwd_product_traffic t
         JOIN dwd_product_conversion c ON t.product_id = c.product_id AND t.date_key = c.date_key
         JOIN dwd_product_engagement e ON t.product_id = e.product_id AND t.date_key = e.date_key
         JOIN dwd_product_acquisition a ON t.product_id = a.product_id AND t.date_key = a.date_key
         JOIN dwd_product_service s ON t.product_id = s.product_id AND t.date_key = s.date_key;