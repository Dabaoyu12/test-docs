-- 切换到指定数据库 (Hive 中 USE 语句用法相同)
USE gmall_work_01;

-- 商品维度表
CREATE TABLE dim_item (
    item_id BIGINT COMMENT '商品ID',
    item_name STRING COMMENT '商品名称',
    category_id INT COMMENT '叶子类目ID',
    category_name STRING COMMENT '叶子类目名称',
    parent_category_id INT COMMENT '父类目ID',
    parent_category_name STRING COMMENT '父类目名称',
    price DECIMAL(10,2) COMMENT '商品价格',
    is_active TINYINT COMMENT '是否在售',
    etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT '商品维度表'
    STORED AS ORC;

-- 类目维度表
DROP TABLE IF EXISTS dim_category;
CREATE TABLE dim_category (
    category_id INT COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    level TINYINT COMMENT '类目层级(1-一级 2-二级 3-叶子)',
    parent_id INT COMMENT '父类目ID',
    path STRING COMMENT '类目路径',
    etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT '类目维度表'
    STORED AS ORC;

-- 用户维度表
CREATE TABLE dim_user (
    user_id BIGINT COMMENT '用户ID',
    user_name STRING COMMENT '用户名',
    reg_date DATE COMMENT '注册日期',
    is_vip TINYINT COMMENT '是否VIP',
    last_payment_date DATE COMMENT '最近支付日期',
    etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT '用户维度表'
    STORED AS ORC;

-- 时间维度表
CREATE TABLE dim_date (
    date_key INT COMMENT '日期键YYYYMMDD',
    date_value DATE COMMENT '日期',
    day TINYINT COMMENT '日',
    month TINYINT COMMENT '月',
    year SMALLINT COMMENT '年',
    week_of_year TINYINT COMMENT '年内周序',
    quarter TINYINT COMMENT '季度',
    is_weekend TINYINT COMMENT '是否周末',
    etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT '时间维度表'
    STORED AS ORC;

-- 终端维度表
CREATE TABLE dim_terminal (
    terminal_code STRING COMMENT '终端代码',
    terminal_name STRING COMMENT '终端名称',
    device_type STRING COMMENT '设备类型',
    etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT '终端维度表'
    STORED AS ORC;

-- 活动维度表
CREATE TABLE dim_activity (
    activity_id STRING COMMENT '活动ID',
    activity_name STRING COMMENT '活动名称',
    activity_type STRING COMMENT '活动类型',
    presale_stage STRING COMMENT '预售阶段',
    start_date DATE COMMENT '活动开始日期',
    end_date DATE COMMENT '活动结束日期',
    etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT '活动维度表'
    STORED AS ORC;

-- 支付方式维度表
CREATE TABLE dim_payment_method (
          method_id STRING COMMENT '支付方式ID',
          method_name STRING COMMENT '支付方式名称',
          is_online TINYINT COMMENT '是否在线支付',
          etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT '支付方式维度表'
    STORED AS ORC;

-- 活动类型维度表
CREATE TABLE dim_activity_type (
         activity_type STRING COMMENT '活动类型',
         activity_name STRING COMMENT '活动名称',
         etl_time TIMESTAMP COMMENT 'ETL时间'
) COMMENT '活动类型维度表'
    STORED AS ORC;

-- 填充商品维度表
INSERT INTO dim_item
SELECT
    item_id,
    CONCAT('商品', item_id),
    FLOOR(RAND()*100)+1,
    CONCAT('类目', FLOOR(RAND()*100)+1),
    FLOOR(RAND()*10)+1,
    CONCAT('父类', FLOOR(RAND()*10)+1),
    ROUND(RAND()*500+10, 2),
    1,  -- 默认在售
    CURRENT_TIMESTAMP()
FROM (
         SELECT DISTINCT item_id
         FROM ods_event_item_visit
         LIMIT 2000
     ) t;

-- 填充类目维度表
INSERT INTO dim_category
WITH categories AS (
    SELECT
        100 + root_id AS category_id,
        CONCAT('Root Category ', root_id) AS category_name,
        1 AS level,
        NULL AS parent_id,
        CAST(100 + root_id AS STRING) AS path
    FROM (SELECT 1 AS root_id UNION ALL SELECT 2 UNION ALL SELECT 3) root_cat

    UNION ALL

    SELECT
        1000 + root_id * 10 + sub_id AS category_id,
        CONCAT('Subcategory ', root_id, '-', sub_id) AS category_name,
        2 AS level,
        100 + root_id AS parent_id,
        CONCAT_WS('.', CAST(100 + root_id AS STRING), CAST(1000 + root_id*10 + sub_id AS STRING)) AS path
    FROM (
             SELECT roots.root_id, subs.sub_id
             FROM (SELECT 1 AS root_id UNION ALL SELECT 2 UNION ALL SELECT 3) roots
                      CROSS JOIN (SELECT 1 AS sub_id UNION ALL SELECT 2) subs
         ) sub_cat
)
SELECT
    category_id,
    category_name,
    level,
    parent_id,
    path,
    CURRENT_TIMESTAMP()
FROM categories;

-- 填充用户维度表
INSERT INTO dim_user
SELECT
    user_id,
    CONCAT('用户', user_id),
    DATE_SUB(CURRENT_DATE(), CAST(FLOOR(RAND()*1000) AS INT)),
    IF(RAND() > 0.7, 1, 0),
    IF(RAND() > 0.3, DATE_SUB(CURRENT_DATE(), CAST(FLOOR(RAND()*100) AS INT)), NULL),
    CURRENT_TIMESTAMP()
FROM (
         SELECT DISTINCT user_id
         FROM ods_event_item_visit
         WHERE user_id IS NOT NULL
         LIMIT 5000
     ) t;

-- 填充时间维度表
INSERT INTO dim_date
SELECT
    CAST(DATE_FORMAT(dt, 'yyyyMMdd') AS INT),
    dt,
    DAY(dt),
    MONTH(dt),
    YEAR(dt),
    WEEKOFYEAR(dt),
    QUARTER(dt),
    IF(DAYOFWEEK(dt) IN (1,7), 1, 0),
    CURRENT_TIMESTAMP()
FROM (
         SELECT DATE_SUB(CURRENT_DATE(), n) AS dt
         FROM (
                  SELECT pos AS n
                  FROM (SELECT split(space(400), ' ') AS arr) tmp
     LATERAL VIEW posexplode(tmp.arr) exploded_table AS pos, val
              ) seq
     ) dates;

-- 填充支付方式维度表
INSERT INTO dim_payment_method
SELECT
    DISTINCT payment_method,
             CASE payment_method
                 WHEN 'ALIPAY' THEN '支付宝'
                 WHEN 'WECHAT' THEN '微信支付'
                 WHEN 'CCB' THEN '建设银行'
                 ELSE '其他支付'
                 END,
             1,
             CURRENT_TIMESTAMP()
FROM ods_event_payment;

-- 填充终端维度表
INSERT INTO dim_terminal
SELECT
    DISTINCT terminal,
             CASE terminal
                 WHEN 'PC' THEN '电脑端'
                 WHEN 'Wireless' THEN '无线端'
                 WHEN 'App' THEN '应用端'
                 ELSE terminal
                 END,
             CASE terminal
                 WHEN 'PC' THEN 'PC'
                 ELSE 'Mobile'
                 END,
             CURRENT_TIMESTAMP()
FROM (
         SELECT terminal FROM ods_event_item_visit
         UNION
         SELECT terminal FROM ods_event_item_pv
         UNION
         SELECT terminal FROM ods_event_item_micro_detail
     ) t;

-- 填充活动类型维度表
INSERT INTO dim_activity_type
SELECT
    activity_type,
    CASE activity_type
        WHEN 'NORMAL' THEN '普通活动'
        WHEN 'PRESALE' THEN '预售活动'
        WHEN 'JUHUASUAN' THEN '聚划算'
        ELSE activity_type
        END,
    CURRENT_TIMESTAMP()
FROM (
         SELECT
             CASE
                 WHEN is_juhuasuan = 1 THEN 'JUHUASUAN'
                 WHEN is_presale = 1 THEN 'PRESALE'
                 ELSE 'NORMAL'
                 END AS activity_type
         FROM ods_event_payment
         GROUP BY activity_type
     ) t;

-- 填充活动维度表
INSERT INTO dim_activity
SELECT
    'NORMAL' AS activity_id,
    '普通商品活动' AS activity_name,
    'NORMAL' AS activity_type,
    NULL AS presale_stage,
    DATE_SUB(CURRENT_DATE(), 60),
    DATE_ADD(CURRENT_DATE(), 30),
    CURRENT_TIMESTAMP()
WHERE NOT EXISTS (
    SELECT 1 FROM dim_activity WHERE activity_id = 'NORMAL'
);

INSERT INTO dim_activity
SELECT
    CONCAT('PRESALE-', stage),
    CONCAT('预售活动-',
           CASE stage
               WHEN 'DEPOSIT' THEN '定金阶段'
               WHEN 'FINAL' THEN '尾款阶段'
               ELSE '全款阶段'
               END),
    'PRESALE',
    stage,
    DATE_SUB(CURRENT_DATE(), 30),
    DATE_ADD(CURRENT_DATE(), 15),
    CURRENT_TIMESTAMP()
FROM (
         SELECT DISTINCT presale_stage AS stage
         FROM ods_event_payment
         WHERE is_presale = 1 AND presale_stage IS NOT NULL
     ) stages;

INSERT INTO dim_activity
SELECT
    'JUHUASUAN',
    '聚划算活动',
    'JUHUASUAN',
    NULL,
    DATE_SUB(CURRENT_DATE(), 15),
    DATE_ADD(CURRENT_DATE(), 7),
    CURRENT_TIMESTAMP()
WHERE NOT EXISTS (
    SELECT 1 FROM dim_activity WHERE activity_id = 'JUHUASUAN'
);