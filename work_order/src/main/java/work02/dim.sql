USE gmall_work_02;

-- 1. dim_date
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
                          date_id         DATE         PRIMARY KEY COMMENT '日期ID',
                          day_of_week     VARCHAR(10)  COMMENT '星期几',
                          is_weekend      TINYINT(1)   COMMENT '是否周末',
                          week_of_year    INT          COMMENT '一年中的第几周',
                          month           TINYINT      COMMENT '月',
                          quarter         TINYINT      COMMENT '季度',
                          year            SMALLINT     COMMENT '年'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DIM: 日期维度';

-- 2. dim_category
DROP TABLE IF EXISTS dim_category;
CREATE TABLE dim_category (
                              category_id      INT         PRIMARY KEY COMMENT '类目ID',
                              category_level   TINYINT     COMMENT '类目层级(1-3)',
                              category_name    VARCHAR(100) COMMENT '类目名称',
                              parent_id        INT         COMMENT '父类目ID'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DIM: 商品类目维度';

-- 3. dim_brand
DROP TABLE IF EXISTS dim_brand;
CREATE TABLE dim_brand (
                           brand_id      BIGINT      PRIMARY KEY COMMENT '品牌ID',
                           brand_name    VARCHAR(100) COMMENT '品牌名称'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DIM: 品牌维度';

-- 4. dim_product
DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
                             product_id       BIGINT      PRIMARY KEY COMMENT '商品ID',
                             product_name     VARCHAR(200) COMMENT '商品名称',
                             brand_id         BIGINT      COMMENT '品牌ID',
                             vendor_id        BIGINT      COMMENT '供应商ID',
                             first_cat_id     INT         COMMENT '一级类目ID',
                             second_cat_id    INT         COMMENT '二级类目ID',
                             third_cat_id     INT         COMMENT '三级类目ID',
                             create_time      DATETIME    COMMENT '商品上架时间',
                             INDEX idx_prod_brand (brand_id),
                             INDEX idx_prod_cat1 (first_cat_id),
                             INDEX idx_prod_cat2 (second_cat_id),
                             INDEX idx_prod_cat3 (third_cat_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DIM: 商品维度';

-- 5. dim_sku
DROP TABLE IF EXISTS dim_sku;
CREATE TABLE dim_sku (
                         sku_id           BIGINT      PRIMARY KEY COMMENT 'SKU ID',
                         product_id       BIGINT      COMMENT '关联商品ID',
                         sku_name         VARCHAR(200) COMMENT 'SKU属性',
                         create_time      DATETIME    COMMENT 'SKU创建时间',
                         INDEX idx_sku_prod (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DIM: SKU维度';

-- 6. dim_traffic_source
DROP TABLE IF EXISTS dim_traffic_source;
CREATE TABLE dim_traffic_source (
                                    source_type   VARCHAR(50) PRIMARY KEY COMMENT '流量来源编码',
                                    source_name   VARCHAR(100) COMMENT '流量来源名称'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DIM: 流量来源维度';



-- 1. 填充 dim_date （示例：2025-01-01 到 2025-12-31）
INSERT INTO dim_date (date_id, day_of_week, is_weekend, week_of_year, month, quarter, year)
SELECT
    d AS date_id,
    DATE_FORMAT(d, '%W') AS day_of_week,
    IF(DAYOFWEEK(d) IN (1,7), 1, 0) AS is_weekend,
    WEEK(d, 1) AS week_of_year,
    MONTH(d) AS month,
    QUARTER(d) AS quarter,
    YEAR(d) AS year
FROM (
         SELECT DATE_ADD('2025-01-01', INTERVAL t.n DAY) AS d
         FROM (
                  SELECT a.n + b.n * 10 + c.n * 100 AS n
                  FROM (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
                        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
                           CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
                                       UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
                           CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) c
              ) t
         WHERE DATE_ADD('2025-01-01', INTERVAL t.n DAY) <= '2025-12-31'
     ) dates;

-- 2. 填充 dim_category （请根据实际类目结构调整）
SET FOREIGN_KEY_CHECKS=0;
TRUNCATE TABLE dim_category;
SET FOREIGN_KEY_CHECKS=1;

INSERT INTO dim_category (category_id, category_level, category_name, parent_id)
SELECT
    m.id                  AS category_id,
    m.level               AS category_level,
    ANY_VALUE(r.name)     AS category_name,
    ANY_VALUE(r.parent_id) AS parent_id
FROM (
         -- 原始候选行
         SELECT first_cat_id AS id, 1 AS level,
                CAST(first_cat_id AS CHAR) AS name,
                NULL AS parent_id
         FROM ods_product_info WHERE first_cat_id IS NOT NULL

         UNION ALL

         SELECT second_cat_id AS id, 2 AS level,
                CONCAT(CAST(first_cat_id AS CHAR), ' > ', CAST(second_cat_id AS CHAR)) AS name,
                first_cat_id AS parent_id
         FROM ods_product_info WHERE second_cat_id IS NOT NULL

         UNION ALL

         SELECT third_cat_id AS id, 3 AS level,
                CONCAT(
                        CAST(first_cat_id AS CHAR), ' > ',
                        CAST(second_cat_id AS CHAR), ' > ',
                        CAST(third_cat_id AS CHAR)
                ) AS name,
                second_cat_id AS parent_id
         FROM ods_product_info WHERE third_cat_id IS NOT NULL
     ) AS r
         JOIN (
    -- 每个 id 的最高层级
    SELECT x.id, MAX(x.level) AS level
    FROM (
             SELECT first_cat_id AS id, 1 AS level FROM ods_product_info WHERE first_cat_id IS NOT NULL
             UNION ALL
             SELECT second_cat_id AS id, 2 AS level FROM ods_product_info WHERE second_cat_id IS NOT NULL
             UNION ALL
             SELECT third_cat_id AS id, 3 AS level FROM ods_product_info WHERE third_cat_id IS NOT NULL
         ) AS x
    GROUP BY x.id
) AS m
              ON r.id = m.id AND r.level = m.level
GROUP BY m.id, m.level;

-- 3. 填充 dim_brand （请替换为品牌全量列表或同步脚本）
INSERT INTO dim_brand (brand_id, brand_name)
SELECT DISTINCT brand_id, CONCAT('品牌-', brand_id)
FROM ods_product_info;

-- 4. 填充 dim_product
INSERT INTO dim_product (product_id, product_name, brand_id, vendor_id, first_cat_id, second_cat_id, third_cat_id, create_time)
SELECT
    product_id, product_name, brand_id, vendor_id,
    first_cat_id, second_cat_id, third_cat_id, create_time
FROM ods_product_info;

-- 5. 填充 dim_sku
INSERT INTO dim_sku (sku_id, product_id, sku_name, create_time)
SELECT sku_id, product_id, sku_name, create_time
FROM ods_sku_info;

-- 6. 填充 dim_traffic_source
INSERT INTO dim_traffic_source (source_type, source_name)
SELECT DISTINCT source_type,
                source_type AS source_name
FROM ods_traffic_source_log;
