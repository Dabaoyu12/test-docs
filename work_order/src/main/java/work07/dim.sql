-- ====================================================================
-- DIM层表创建脚本
-- 工单编号：大数据-电商数仓-07-商品主题商品诊断看板
-- ====================================================================

use gmall_work_07;
SET FOREIGN_KEY_CHECKS = 0;

-- 1. 商品维度表 dim_product

CREATE TABLE IF NOT EXISTS dw_dim_product (
    product_key     BIGINT       NOT NULL AUTO_INCREMENT COMMENT '商品代理主键，自增',
    product_id      BIGINT       NOT NULL COMMENT '商品自然主键，来源 ods_dim_product.product_id',
    product_name    VARCHAR(255) NOT NULL COMMENT '商品名称',
    category_id     BIGINT       NOT NULL COMMENT '类目 ID',
    category_name   VARCHAR(100) NOT NULL COMMENT '类目名称',
    brand_id        BIGINT       NOT NULL COMMENT '品牌 ID',
    brand_name      VARCHAR(100) NOT NULL COMMENT '品牌名称',
    launch_date     DATE         NOT NULL COMMENT '上市日期',
    attributes      JSON         NULL COMMENT '商品属性（JSON）',
    status          ENUM('ON','OFF') NOT NULL COMMENT '状态：ON 在售，OFF 下架',
    effective_date  DATE         NOT NULL COMMENT '记录生效日期',
    end_date        DATE         NOT NULL COMMENT '记录失效日期',
    current_flag    CHAR(1)      NOT NULL DEFAULT 'Y' COMMENT '是否当前记录：Y 是，N 否',
    create_time     DATETIME     NOT NULL COMMENT 'ODS 原始记录创建时间',
    update_time     DATETIME     NOT NULL COMMENT 'ODS 原始记录更新时间',
    PRIMARY KEY (product_key),
    UNIQUE KEY uk_dim_product (product_id, effective_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品维度表（SCD2）';

INSERT INTO dw_dim_product (
    product_id, product_name, category_id, category_name,
    brand_id, brand_name, launch_date, attributes, status,
    effective_date, end_date, current_flag, create_time, update_time
)
SELECT
    product_id, product_name, category_id, category_name,
    brand_id, brand_name, launch_date, attributes, status,
    CURRENT_DATE        AS effective_date,
    '9999-12-31'        AS end_date,
    'Y'                 AS current_flag,
    create_time, update_time
FROM ods_dim_product;

-- 2. SKU 维度表 dim_sku

CREATE TABLE IF NOT EXISTS dw_dim_sku (
    sku_key      BIGINT       NOT NULL AUTO_INCREMENT COMMENT 'SKU 代理主键，自增',
    sku_id       BIGINT       NOT NULL COMMENT 'SKU 自然主键，来源 ods_dim_sku.sku_id',
    product_key  BIGINT       NOT NULL COMMENT '关联 dw_dim_product.product_key',
    sku_code     VARCHAR(64)  NOT NULL COMMENT 'SKU 编码',
    color        VARCHAR(50)  NULL COMMENT '颜色',
    size         VARCHAR(50)  NULL COMMENT '尺寸',
    packaging    VARCHAR(100) NULL COMMENT '包装信息',
    weight       DECIMAL(10,3) NULL COMMENT '重量（kg）',
    price        DECIMAL(10,2) NOT NULL COMMENT '销售价',
    status       ENUM('ON','OFF') NOT NULL COMMENT '状态：ON 有货，OFF 无货',
    effective_date  DATE      NOT NULL COMMENT '生效日期',
    end_date        DATE      NOT NULL COMMENT '失效日期',
    current_flag    CHAR(1)   NOT NULL DEFAULT 'Y' COMMENT '当前标志 Y/N',
    create_time     DATETIME  NOT NULL COMMENT 'ODS 创建时间',
    update_time     DATETIME  NOT NULL COMMENT 'ODS 更新时间',
    PRIMARY KEY (sku_key),
    UNIQUE KEY uk_dim_sku (sku_id, effective_date),
    FOREIGN KEY (product_key) REFERENCES dw_dim_product(product_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SKU 维度表（SCD2）';

INSERT INTO dw_dim_sku (
    sku_id, product_key, sku_code, color, size, packaging,
    weight, price, status,
    effective_date, end_date, current_flag, create_time, update_time
)
SELECT
    s.sku_id,
    p.product_key,
    s.sku_code, s.color, s.size, s.packaging,
    s.weight, s.price, s.status,
    CURRENT_DATE        AS effective_date,
    '9999-12-31'        AS end_date,
    'Y'                 AS current_flag,
    s.create_time, s.update_time
FROM ods_dim_sku s
         JOIN dw_dim_product p
              ON s.product_id = p.product_id;


-- 3. 供应商维度表 dim_vendor

CREATE TABLE IF NOT EXISTS dw_dim_vendor (
    vendor_key    BIGINT       NOT NULL AUTO_INCREMENT COMMENT '供应商代理主键',
    vendor_id     BIGINT       NOT NULL COMMENT '自然主键，来源 ods_dim_vendor.vendor_id',
    vendor_name   VARCHAR(200) NOT NULL COMMENT '供应商名称',
    vendor_level  VARCHAR(20)  NULL COMMENT '供应商等级',
    rating_score  DECIMAL(3,2) NULL COMMENT '信誉评分',
    contact_info  JSON         NULL COMMENT '联系方式（JSON）',
    effective_date  DATE       NOT NULL COMMENT '生效日期',
    end_date        DATE       NOT NULL COMMENT '失效日期',
    current_flag    CHAR(1)    NOT NULL DEFAULT 'Y' COMMENT '当前标志 Y/N',
    create_time     DATETIME   NOT NULL COMMENT 'ODS 创建时间',
    update_time     DATETIME   NOT NULL COMMENT 'ODS 更新时间',
    PRIMARY KEY (vendor_key),
    UNIQUE KEY uk_dim_vendor (vendor_id, effective_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='供应商维度表（SCD2）';

INSERT INTO dw_dim_vendor (
    vendor_id, vendor_name, vendor_level, rating_score, contact_info,
    effective_date, end_date, current_flag, create_time, update_time
)
SELECT
    vendor_id, vendor_name, vendor_level, rating_score, contact_info,
    CURRENT_DATE       AS effective_date,
    '9999-12-31'       AS end_date,
    'Y'                AS current_flag,
    create_time, update_time
FROM ods_dim_vendor;


-- 4. 日期维度表 dim_date

CREATE TABLE IF NOT EXISTS dw_dim_date (
    date_key      DATE       NOT NULL COMMENT '日期，主键',
    day_of_week   TINYINT    NOT NULL COMMENT '周几，1=Monday…7=Sunday',
    day_of_month  TINYINT    NOT NULL COMMENT '月中天',
    month         TINYINT    NOT NULL COMMENT '月份',
    quarter       TINYINT    NOT NULL COMMENT '季度',
    year          SMALLINT   NOT NULL COMMENT '年份',
    is_weekend    BOOLEAN    NOT NULL COMMENT '是否周末',
    create_time   DATETIME   NOT NULL COMMENT 'ODS 创建时间',
    PRIMARY KEY (date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='日期维度表';

INSERT INTO dw_dim_date
SELECT * FROM ods_dim_date;


-- 5. 用户维度表 dim_user

CREATE TABLE IF NOT EXISTS dw_dim_user (
    user_key       BIGINT      NOT NULL AUTO_INCREMENT COMMENT '用户代理主键',
    user_id        BIGINT      NOT NULL COMMENT '自然主键，来源 ods_dim_user.user_id',
    register_time  DATETIME    NOT NULL COMMENT '注册时间',
    register_channel VARCHAR(50) NULL COMMENT '注册渠道',
    user_level     VARCHAR(20) NULL COMMENT '用户分层',
    city           VARCHAR(50) NULL COMMENT '所在城市',
    age_group      VARCHAR(20) NULL COMMENT '年龄段',
    effective_date DATE        NOT NULL COMMENT '生效日期',
    end_date       DATE        NOT NULL COMMENT '失效日期',
    current_flag   CHAR(1)     NOT NULL DEFAULT 'Y' COMMENT '当前标志 Y/N',
    create_time    DATETIME    NOT NULL COMMENT 'ODS 创建时间',
    PRIMARY KEY (user_key),
    UNIQUE KEY uk_dim_user (user_id, effective_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户维度表（SCD2）';

INSERT INTO dw_dim_user (
    user_id, register_time, register_channel, user_level, city, age_group,
    effective_date, end_date, current_flag, create_time
)
SELECT
    user_id, register_time, register_channel, user_level, city, age_group,
    CURRENT_DATE      AS effective_date,
    '9999-12-31'      AS end_date,
    'Y'               AS current_flag,
    create_time
FROM ods_dim_user;

-- 6. 竞品商品维度表 dim_competitor_product

CREATE TABLE IF NOT EXISTS dw_dim_competitor_product (
    comp_product_key   BIGINT       NOT NULL AUTO_INCREMENT COMMENT '竞品商品代理主键',
    comp_product_id    BIGINT       NOT NULL COMMENT '自然主键，来源 ods_dim_competitor_product.comp_product_id',
    comp_name          VARCHAR(255) NOT NULL COMMENT '竞品名称',
    comp_category_id   BIGINT       NOT NULL COMMENT '竞品类目 ID',
    comp_category_name VARCHAR(100) NOT NULL COMMENT '竞品类目名称',
    comp_price         DECIMAL(10,2) NULL COMMENT '竞品价格',
    comp_brand         VARCHAR(100) NULL COMMENT '竞品品牌',
    effective_date     DATE         NOT NULL COMMENT '生效日期',
    end_date           DATE         NOT NULL COMMENT '失效日期',
    current_flag       CHAR(1)      NOT NULL DEFAULT 'Y' COMMENT '当前标志 Y/N',
    update_time        DATETIME     NOT NULL COMMENT 'ODS 更新时间',
    PRIMARY KEY (comp_product_key),
    UNIQUE KEY uk_dim_competitor (comp_product_id, effective_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='竞品商品维度表（SCD2）';

INSERT INTO dw_dim_competitor_product (
    comp_product_id, comp_name, comp_category_id, comp_category_name,
    comp_price, comp_brand, effective_date, end_date, current_flag, update_time
)
SELECT
    comp_product_id, comp_name, comp_category_id, comp_category_name,
    comp_price, comp_brand,
    CURRENT_DATE      AS effective_date,
    '9999-12-31'      AS end_date,
    'Y'               AS current_flag,
    update_time
FROM ods_dim_competitor_product;

-- Re-enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;
