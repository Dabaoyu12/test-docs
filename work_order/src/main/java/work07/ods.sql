create database if not exists gmall_work_07;
use gmall_work_07;
-- 1. 商品维度表
CREATE TABLE if not exists `ods_dim_product` (
                                                 `product_id`    BIGINT       NOT NULL AUTO_INCREMENT COMMENT '商品主键，自增',
                                                 `product_name`  VARCHAR(255) NOT NULL COMMENT '商品名称',
                                                 `category_id`   BIGINT       NOT NULL COMMENT '类目ID，关联分类维度',
                                                 `category_name` VARCHAR(100) NOT NULL COMMENT '类目名称',
                                                 `brand_id`      BIGINT       NOT NULL COMMENT '品牌ID，关联品牌维度',
                                                 `brand_name`    VARCHAR(100) NOT NULL COMMENT '品牌名称',
                                                 `launch_date`   DATE         NOT NULL COMMENT '上市日期',
                                                 `attributes`    JSON         NULL COMMENT '商品属性，JSON格式（如颜色、材质等）',
                                                 `status`        ENUM('ON','OFF') NOT NULL DEFAULT 'ON' COMMENT '状态：ON在售，OFF下架',
                                                 `create_time`   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                                 `update_time`   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
                                                 PRIMARY KEY (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品基础信息维度表';

-- 2. SKU 维度表
CREATE TABLE `ods_dim_sku` (
                               `sku_id`      BIGINT       NOT NULL AUTO_INCREMENT COMMENT 'SKU主键，自增',
                               `product_id`  BIGINT       NOT NULL COMMENT '关联商品ID',
                               `sku_code`    VARCHAR(64)  NOT NULL COMMENT 'SKU编码',
                               `color`       VARCHAR(50)  NULL COMMENT '颜色',
                               `size`        VARCHAR(50)  NULL COMMENT '尺寸',
                               `packaging`   VARCHAR(100) NULL COMMENT '包装信息',
                               `weight`      DECIMAL(10,3) NULL COMMENT '重量（kg）',
                               `price`       DECIMAL(10,2) NOT NULL COMMENT '销售价',
                               `status`      ENUM('ON','OFF') NOT NULL DEFAULT 'ON' COMMENT '状态：ON有货，OFF无货',
                               `create_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                               `update_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                               PRIMARY KEY (`sku_id`),
                               KEY `idx_product` (`product_id`),
                               CONSTRAINT `fk_sku_product` FOREIGN KEY (`product_id`) REFERENCES `ods_dim_product`(`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SKU 维度表';

-- 3. 供应商维度表
CREATE TABLE `ods_dim_vendor` (
                                  `vendor_id`     BIGINT       NOT NULL AUTO_INCREMENT COMMENT '供应商ID，自增',
                                  `vendor_name`   VARCHAR(200) NOT NULL COMMENT '供应商名称',
                                  `vendor_level`  VARCHAR(20)  NULL COMMENT '供应商等级',
                                  `rating_score`  DECIMAL(3,2) NULL COMMENT '信誉评分',
                                  `contact_info`  JSON         NULL COMMENT '联系方式，JSON格式',
                                  `create_time`   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                                  `update_time`   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                                  PRIMARY KEY (`vendor_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='供应商/商家维度表';

-- 4. 日期维度表
CREATE TABLE `ods_dim_date` (
                                `date_key`     DATE       NOT NULL COMMENT '日期，主键',
                                `day_of_week`  TINYINT    NOT NULL COMMENT '周几，1=Monday…7=Sunday',
                                `day_of_month` TINYINT    NOT NULL COMMENT '月中的天',
                                `month`        TINYINT    NOT NULL COMMENT '月份',
                                `quarter`      TINYINT    NOT NULL COMMENT '季度',
                                `year`         SMALLINT   NOT NULL COMMENT '年份',
                                `is_weekend`   BOOLEAN    NOT NULL COMMENT '是否周末',
                                `create_time`  DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                                PRIMARY KEY (`date_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='日期维度表';

-- 5. 用户维度表
CREATE TABLE `ods_dim_user` (
                                `user_id`         BIGINT      NOT NULL AUTO_INCREMENT COMMENT '用户ID，自增',
                                `register_time`   DATETIME    NOT NULL COMMENT '注册时间',
                                `register_channel` VARCHAR(50) NULL COMMENT '注册渠道',
                                `user_level`      VARCHAR(20) NULL COMMENT '用户分层，例如NEW/ACTIVE/VIP',
                                `city`            VARCHAR(50) NULL COMMENT '所在城市',
                                `age_group`       VARCHAR(20) NULL COMMENT '年龄段',
                                `create_time`     DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                                PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户维度表';

-- 6. 竞品商品维度表
CREATE TABLE `ods_dim_competitor_product` (
                                              `comp_product_id`    BIGINT       NOT NULL AUTO_INCREMENT COMMENT '竞品商品ID，自增',
                                              `comp_name`          VARCHAR(255) NOT NULL COMMENT '竞品商品名称',
                                              `comp_category_id`   BIGINT       NOT NULL COMMENT '竞品类目ID',
                                              `comp_category_name` VARCHAR(100) NOT NULL COMMENT '竞品类目名称',
                                              `comp_price`         DECIMAL(10,2) NULL COMMENT '竞品价格',
                                              `comp_brand`         VARCHAR(100) NULL COMMENT '竞品品牌',
                                              `update_time`        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                                              PRIMARY KEY (`comp_product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='竞品商品维度表';

-- 二、事实表

-- 7. 页面浏览日志表
CREATE TABLE `ods_fact_pageview` (
                                     `pv_id`       BIGINT      NOT NULL AUTO_INCREMENT COMMENT 'PV日志ID，自增',
                                     `user_id`     BIGINT      NOT NULL COMMENT '用户ID，关联用户维度',
                                     `product_id`  BIGINT      NOT NULL COMMENT '商品ID，关联商品维度',
                                     `session_id`  VARCHAR(64) NOT NULL COMMENT '会话ID',
                                     `page_url`    VARCHAR(255) NULL COMMENT '访问页面URL',
                                     `referer`     VARCHAR(255) NULL COMMENT '来源网页',
                                     `visit_time`  DATETIME    NOT NULL COMMENT '访问时间',
                                     `device_type` VARCHAR(20) NULL COMMENT '设备类型',
                                     `browser`     VARCHAR(50) NULL COMMENT '浏览器',
                                     `city`        VARCHAR(50) NULL COMMENT '访问城市',
                                     `date_key`    DATE        NOT NULL COMMENT '关联日期维度',
                                     `create_time` DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                     PRIMARY KEY (`pv_id`),
                                     KEY `idx_pageview_user` (`user_id`),
                                     KEY `idx_pageview_prod` (`product_id`),
                                     KEY `idx_pageview_date` (`date_key`),
                                     CONSTRAINT `fk_pv_user` FOREIGN KEY (`user_id`) REFERENCES `ods_dim_user`(`user_id`),
                                     CONSTRAINT `fk_pv_prod` FOREIGN KEY (`product_id`) REFERENCES `ods_dim_product`(`product_id`),
                                     CONSTRAINT `fk_pv_date` FOREIGN KEY (`date_key`) REFERENCES `ods_dim_date`(`date_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='页面浏览日志（PV）';

-- 8. 独立访客日志表
CREATE TABLE `ods_fact_uv` (
                               `uv_id`       BIGINT      NOT NULL AUTO_INCREMENT COMMENT 'UV日志ID，自增',
                               `ip_address`  VARCHAR(45) NULL COMMENT 'IP地址',
                               `cookie_id`   VARCHAR(128) NULL COMMENT 'Cookie标识',
                               `user_id`     BIGINT      NULL COMMENT '用户ID，关联用户维度',
                               `date_key`    DATE        NOT NULL COMMENT '关联日期维度',
                               `product_id`  BIGINT      NOT NULL COMMENT '商品ID，关联商品维度',
                               `channel`     VARCHAR(50) NULL COMMENT '访问渠道',
                               `device_type` VARCHAR(20) NULL COMMENT '设备类型',
                               `create_time` DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                               PRIMARY KEY (`uv_id`),
                               KEY `idx_uv_user` (`user_id`),
                               KEY `idx_uv_prod` (`product_id`),
                               KEY `idx_uv_date` (`date_key`),
                               CONSTRAINT `fk_uv_user` FOREIGN KEY (`user_id`) REFERENCES `ods_dim_user`(`user_id`),
                               CONSTRAINT `fk_uv_date` FOREIGN KEY (`date_key`) REFERENCES `ods_dim_date`(`date_key`),
                               CONSTRAINT `fk_uv_prod` FOREIGN KEY (`product_id`) REFERENCES `ods_dim_product`(`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='独立访客日志（UV）';

-- 9. 流量来源明细表
CREATE TABLE `ods_fact_pv_source` (
                                      `source_id`  BIGINT      NOT NULL AUTO_INCREMENT COMMENT '来源ID，自增',
                                      `product_id` BIGINT      NOT NULL COMMENT '商品ID，关联商品维度',
                                      `date_key`   DATE        NOT NULL COMMENT '关联日期维度',
                                      `source_type` ENUM('自然搜索','付费搜索','社交','站外') NOT NULL COMMENT '来源类型',
                                      `pv_count`   BIGINT      NOT NULL COMMENT 'PV数量',
                                      `uv_count`   BIGINT      NOT NULL COMMENT 'UV数量',
                                      `create_time` DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                      PRIMARY KEY (`source_id`),
                                      KEY `idx_pvs_prod` (`product_id`),
                                      KEY `idx_pvs_date` (`date_key`),
                                      CONSTRAINT `fk_pvs_prod` FOREIGN KEY (`product_id`) REFERENCES `ods_dim_product`(`product_id`),
                                      CONSTRAINT `fk_pvs_date` FOREIGN KEY (`date_key`) REFERENCES `ods_dim_date`(`date_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='流量来源明细';

-- 10. 加购事件表
CREATE TABLE `ods_fact_cart_add` (
                                     `cart_add_id` BIGINT      NOT NULL AUTO_INCREMENT COMMENT '加购ID，自增',
                                     `user_id`     BIGINT      NOT NULL COMMENT '用户ID，关联用户维度',
                                     `product_id`  BIGINT      NOT NULL COMMENT '商品ID，关联商品维度',
                                     `sku_id`      BIGINT      NOT NULL COMMENT 'SKU ID，关联SKU维度',
                                     `session_id`  VARCHAR(64) NULL COMMENT '会话ID',
                                     `add_time`    DATETIME    NOT NULL COMMENT '加购时间',
                                     `quantity`    INT         NOT NULL COMMENT '加购数量',
                                     `date_key`    DATE        NOT NULL COMMENT '关联日期维度',
                                     `create_time` DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                     PRIMARY KEY (`cart_add_id`),
                                     KEY `idx_cart_user` (`user_id`),
                                     KEY `idx_cart_prod` (`product_id`),
                                     KEY `idx_cart_sku`  (`sku_id`),
                                     KEY `idx_cart_date` (`date_key`),
                                     CONSTRAINT `fk_cart_user` FOREIGN KEY (`user_id`) REFERENCES `ods_dim_user`(`user_id`),
                                     CONSTRAINT `fk_cart_prod` FOREIGN KEY (`product_id`) REFERENCES `ods_dim_product`(`product_id`),
                                     CONSTRAINT `fk_cart_sku`  FOREIGN KEY (`sku_id`) REFERENCES `ods_dim_sku`(`sku_id`),
                                     CONSTRAINT `fk_cart_date` FOREIGN KEY (`date_key`) REFERENCES `ods_dim_date`(`date_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='加购事件日志';

-- 11. 订单主表
CREATE TABLE `ods_fact_order` (
                                  `order_id`     BIGINT      NOT NULL AUTO_INCREMENT COMMENT '订单ID，自增',
                                  `user_id`      BIGINT      NOT NULL COMMENT '用户ID，关联用户维度',
                                  `order_time`   DATETIME    NOT NULL COMMENT '下单时间',
                                  `total_amount` DECIMAL(12,2) NOT NULL COMMENT '订单总金额',
                                  `payment_status` ENUM('PENDING','PAID','FAILED') NOT NULL COMMENT '支付状态',
                                  `order_status`  ENUM('NEW','CONFIRMED','CANCELLED','COMPLETED') NOT NULL COMMENT '订单状态',
                                  `date_key`     DATE        NOT NULL COMMENT '关联日期维度',
                                  `create_time`  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                  `update_time`  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
                                  PRIMARY KEY (`order_id`),
                                  KEY `idx_ord_user` (`user_id`),
                                  KEY `idx_ord_date` (`date_key`),
                                  CONSTRAINT `fk_ord_user` FOREIGN KEY (`user_id`) REFERENCES `ods_dim_user`(`user_id`),
                                  CONSTRAINT `fk_ord_date` FOREIGN KEY (`date_key`) REFERENCES `ods_dim_date`(`date_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单主表';

-- 12. 订单明细表
CREATE TABLE `ods_fact_order_item` (
                                       `order_item_id` BIGINT      NOT NULL AUTO_INCREMENT COMMENT '订单明细ID，自增',
                                       `order_id`      BIGINT      NOT NULL COMMENT '关联订单ID',
                                       `product_id`    BIGINT      NOT NULL COMMENT '商品ID，关联商品维度',
                                       `sku_id`        BIGINT      NOT NULL COMMENT 'SKU ID，关联SKU维度',
                                       `quantity`      INT         NOT NULL COMMENT '购买数量',
                                       `unit_price`    DECIMAL(10,2) NOT NULL COMMENT '单价',
                                       `subtotal_amount` DECIMAL(12,2) NOT NULL COMMENT '小计金额',
                                       `create_time`   DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                       PRIMARY KEY (`order_item_id`),
                                       KEY `idx_item_ord` (`order_id`),
                                       KEY `idx_item_prod` (`product_id`),
                                       KEY `idx_item_sku`  (`sku_id`),
                                       CONSTRAINT `fk_item_ord` FOREIGN KEY (`order_id`) REFERENCES `ods_fact_order`(`order_id`),
                                       CONSTRAINT `fk_item_prod` FOREIGN KEY (`product_id`) REFERENCES `ods_dim_product`(`product_id`),
                                       CONSTRAINT `fk_item_sku`  FOREIGN KEY (`sku_id`) REFERENCES `ods_dim_sku`(`sku_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单明细表';

-- 13. 支付日志表
CREATE TABLE `ods_fact_payment` (
                                    `payment_id`    BIGINT      NOT NULL AUTO_INCREMENT COMMENT '支付ID，自增',
                                    `order_id`      BIGINT      NOT NULL COMMENT '订单ID，关联订单主表',
                                    `payment_method` VARCHAR(50) NOT NULL COMMENT '支付方式',
                                    `payment_time`  DATETIME    NOT NULL COMMENT '支付时间',
                                    `payment_amount` DECIMAL(12,2) NOT NULL COMMENT '支付金额',
                                    `transaction_id` VARCHAR(100) NULL COMMENT '交易流水号',
                                    `create_time`   DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                    PRIMARY KEY (`payment_id`),
                                    KEY `idx_pay_ord` (`order_id`),
                                    CONSTRAINT `fk_pay_ord` FOREIGN KEY (`order_id`) REFERENCES `ods_fact_order`(`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='支付日志';

-- 14. 退货退款日志表
CREATE TABLE `ods_fact_return` (
                                   `return_id`     BIGINT      NOT NULL AUTO_INCREMENT COMMENT '退货ID，自增',
                                   `order_id`      BIGINT      NOT NULL COMMENT '关联订单ID',
                                   `product_id`    BIGINT      NOT NULL COMMENT '商品ID',
                                   `sku_id`        BIGINT      NOT NULL COMMENT 'SKU ID',
                                   `return_time`   DATETIME    NOT NULL COMMENT '退货时间',
                                   `return_quantity` INT       NOT NULL COMMENT '退货数量',
                                   `return_reason` VARCHAR(255) NULL COMMENT '退货原因',
                                   `refund_amount` DECIMAL(12,2) NULL COMMENT '退款金额',
                                   `create_time`   DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                   PRIMARY KEY (`return_id`),
                                   KEY `idx_ret_ord` (`order_id`),
                                   KEY `idx_ret_prod` (`product_id`),
                                   KEY `idx_ret_sku`  (`sku_id`),
                                   CONSTRAINT `fk_ret_ord` FOREIGN KEY (`order_id`) REFERENCES `ods_fact_order`(`order_id`),
                                   CONSTRAINT `fk_ret_prod` FOREIGN KEY (`product_id`) REFERENCES `ods_dim_product`(`product_id`),
                                   CONSTRAINT `fk_ret_sku`  FOREIGN KEY (`sku_id`) REFERENCES `ods_dim_sku`(`sku_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='退货/退款日志';

-- 15. 商品评价表
CREATE TABLE `ods_fact_product_review` (
                                           `review_id`     BIGINT      NOT NULL AUTO_INCREMENT COMMENT '评价ID，自增',
                                           `user_id`       BIGINT      NOT NULL COMMENT '用户ID',
                                           `product_id`    BIGINT      NOT NULL COMMENT '商品ID',
                                           `sku_id`        BIGINT      NULL COMMENT 'SKU ID，可空',
                                           `rating`        TINYINT     NOT NULL COMMENT '评分，1-5',
                                           `review_text`   TEXT        NULL COMMENT '评价内容',
                                           `review_time`   DATETIME    NOT NULL COMMENT '评价时间',
                                           `helpful_count` INT         DEFAULT 0 COMMENT '有用数',
                                           `create_time`   DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                           PRIMARY KEY (`review_id`),
                                           KEY `idx_rev_user` (`user_id`),
                                           KEY `idx_rev_prod` (`product_id`),
                                           KEY `idx_rev_sku`  (`sku_id`),
                                           CONSTRAINT `fk_rev_user` FOREIGN KEY (`user_id`) REFERENCES `ods_dim_user`(`user_id`),
                                           CONSTRAINT `fk_rev_prod` FOREIGN KEY (`product_id`) REFERENCES `ods_dim_product`(`product_id`),
                                           CONSTRAINT `fk_rev_sku`  FOREIGN KEY (`sku_id`) REFERENCES `ods_dim_sku`(`sku_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品评价日志';

-- 16. 内容互动日志表
CREATE TABLE `ods_fact_content_engagement` (
                                               `engagement_id` BIGINT      NOT NULL AUTO_INCREMENT COMMENT '互动ID，自增',
                                               `user_id`       BIGINT      NOT NULL COMMENT '用户ID',
                                               `product_id`    BIGINT      NOT NULL COMMENT '商品ID',
                                               `engagement_type` ENUM('点赞','收藏','分享','咨询') NOT NULL COMMENT '互动类型',
                                               `engagement_time` DATETIME  NOT NULL COMMENT '互动时间',
                                               `platform`      ENUM('PC','H5','App') NULL COMMENT '平台来源',
                                               `create_time`   DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                               PRIMARY KEY (`engagement_id`),
                                               KEY `idx_eng_user` (`user_id`),
                                               KEY `idx_eng_prod` (`product_id`),
                                               CONSTRAINT `fk_eng_user` FOREIGN KEY (`user_id`) REFERENCES `ods_dim_user`(`user_id`),
                                               CONSTRAINT `fk_eng_prod` FOREIGN KEY (`product_id`) REFERENCES `ods_dim_product`(`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='内容互动日志';

-- 17. 促销活动日志表
CREATE TABLE `ods_fact_promotion_event` (
                                            `promo_event_id`    BIGINT      NOT NULL AUTO_INCREMENT COMMENT '活动ID，自增',
                                            `activity_id`       VARCHAR(64) NOT NULL COMMENT '营销活动ID',
                                            `product_id`        BIGINT      NOT NULL COMMENT '商品ID',
                                            `sku_id`            BIGINT      NULL COMMENT 'SKU ID，可空',
                                            `event_type`        ENUM('秒杀','满减','优惠券') NOT NULL COMMENT '活动类型',
                                            `participation_count` INT       DEFAULT 0 COMMENT '参与次数',
                                            `order_count`       INT         DEFAULT 0 COMMENT '成交次数',
                                            `start_time`        DATETIME    NOT NULL COMMENT '活动开始时间',
                                            `end_time`          DATETIME    NOT NULL COMMENT '活动结束时间',
                                            `date_key`          DATE        NOT NULL COMMENT '关联日期维度',
                                            `create_time`       DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                            PRIMARY KEY (`promo_event_id`),
                                            KEY `idx_promo_prod` (`product_id`),
                                            KEY `idx_promo_sku`  (`sku_id`),
                                            KEY `idx_promo_date` (`date_key`),
                                            CONSTRAINT `fk_promo_prod` FOREIGN KEY (`product_id`) REFERENCES `ods_dim_product`(`product_id`),
                                            CONSTRAINT `fk_promo_sku` FOREIGN KEY (`sku_id`) REFERENCES `ods_dim_sku`(`sku_id`),
                                            CONSTRAINT `fk_promo_date` FOREIGN KEY (`date_key`) REFERENCES `ods_dim_date`(`date_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='促销活动日志';

-- 18. 用户注册日志表
CREATE TABLE `ods_fact_user_register` (
                                          `register_id`   BIGINT      NOT NULL AUTO_INCREMENT COMMENT '注册日志ID，自增',
                                          `user_id`       BIGINT      NOT NULL COMMENT '用户ID',
                                          `register_channel` VARCHAR(50) NULL COMMENT '注册渠道',
                                          `register_time` DATETIME    NOT NULL COMMENT '注册时间',
                                          `is_first_order` ENUM('Y','N') NOT NULL COMMENT '是否首单',
                                          `date_key`      DATE        NOT NULL COMMENT '关联日期维度',
                                          `create_time`   DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                          PRIMARY KEY (`register_id`),
                                          KEY `idx_reg_user` (`user_id`),
                                          KEY `idx_reg_date` (`date_key`),
                                          CONSTRAINT `fk_reg_user` FOREIGN KEY (`user_id`) REFERENCES `ods_dim_user`(`user_id`),
                                          CONSTRAINT `fk_reg_date` FOREIGN KEY (`date_key`) REFERENCES `ods_dim_date`(`date_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户注册日志';

-- 19. 推荐拉新日志表
CREATE TABLE `ods_fact_referral` (
                                     `referral_id`      BIGINT      NOT NULL AUTO_INCREMENT COMMENT '推荐ID，自增',
                                     `inviter_user_id`  BIGINT      NOT NULL COMMENT '邀请人用户ID',
                                     `invitee_user_id`  BIGINT      NOT NULL COMMENT '被邀请人用户ID',
                                     `invite_time`      DATETIME    NOT NULL COMMENT '邀请时间',
                                     `first_order_time` DATETIME    NULL COMMENT '首单时间，可空',
                                     `date_key`         DATE        NOT NULL COMMENT '关联日期维度',
                                     `create_time`      DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                     PRIMARY KEY (`referral_id`),
                                     KEY `idx_ref_inviter` (`inviter_user_id`),
                                     KEY `idx_ref_invitee` (`invitee_user_id`),
                                     KEY `idx_ref_date`    (`date_key`),
                                     CONSTRAINT `fk_ref_inviter` FOREIGN KEY (`inviter_user_id`) REFERENCES `ods_dim_user`(`user_id`),
                                     CONSTRAINT `fk_ref_invitee` FOREIGN KEY (`invitee_user_id`) REFERENCES `ods_dim_user`(`user_id`),
                                     CONSTRAINT `fk_ref_date` FOREIGN KEY (`date_key`) REFERENCES `ods_dim_date`(`date_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='推荐/拉新日志';

-- 20. 客服工单表
CREATE TABLE `ods_fact_service_ticket` (
                                           `ticket_id`    BIGINT      NOT NULL AUTO_INCREMENT COMMENT '工单ID，自增',
                                           `user_id`      BIGINT      NOT NULL COMMENT '用户ID',
                                           `product_id`   BIGINT      NULL COMMENT '商品ID，可空',
                                           `ticket_type`  ENUM('咨询','投诉','建议') NOT NULL COMMENT '工单类型',
                                           `create_time`  DATETIME    NOT NULL COMMENT '工单创建时间',
                                           `close_time`   DATETIME    NULL COMMENT '工单关闭时间',
                                           `resolution`   TEXT        NULL COMMENT '处理结果',
                                           `agent_id`     BIGINT      NULL COMMENT '处理人ID',
                                           `status`       ENUM('OPEN','CLOSED','PENDING') NOT NULL COMMENT '工单状态',
                                           `date_key`     DATE        NOT NULL COMMENT '关联日期维度',
                                           PRIMARY KEY (`ticket_id`),
                                           KEY `idx_ticket_user` (`user_id`),
                                           KEY `idx_ticket_prod` (`product_id`),
                                           KEY `idx_ticket_date` (`date_key`),
                                           CONSTRAINT `fk_ticket_user` FOREIGN KEY (`user_id`) REFERENCES `ods_dim_user`(`user_id`),
                                           CONSTRAINT `fk_ticket_prod` FOREIGN KEY (`product_id`) REFERENCES `ods_dim_product`(`product_id`),
                                           CONSTRAINT `fk_ticket_date` FOREIGN KEY (`date_key`) REFERENCES `ods_dim_date`(`date_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客服工单表';

-- 21. 售后服务表
CREATE TABLE `ods_fact_after_sales` (
                                        `aftersales_id`   BIGINT      NOT NULL AUTO_INCREMENT COMMENT '售后ID，自增',
                                        `order_id`        BIGINT      NOT NULL COMMENT '关联订单ID',
                                        `product_id`      BIGINT      NOT NULL COMMENT '商品ID',
                                        `case_type`       ENUM('投诉','维权','返修','换货') NOT NULL COMMENT '售后类型',
                                        `case_time`       DATETIME    NOT NULL COMMENT '事件时间',
                                        `resolution_time` DATETIME    NULL COMMENT '处理完成时间',
                                        `resolution_result` TEXT      NULL COMMENT '处理结果',
                                        `date_key`        DATE        NOT NULL COMMENT '关联日期维度',
                                        PRIMARY KEY (`aftersales_id`),
                                        KEY `idx_as_ord`  (`order_id`),
                                        KEY `idx_as_prod` (`product_id`),
                                        KEY `idx_as_date` (`date_key`),
                                        CONSTRAINT `fk_as_ord` FOREIGN KEY (`order_id`) REFERENCES `ods_fact_order`(`order_id`),
                                        CONSTRAINT `fk_as_prod` FOREIGN KEY (`product_id`) REFERENCES `ods_dim_product`(`product_id`),
                                        CONSTRAINT `fk_as_date` FOREIGN KEY (`date_key`) REFERENCES `ods_dim_date`(`date_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='售后服务记录';

-- 22. 竞品表现表
CREATE TABLE `ods_fact_competitor_performance` (
                                                   `comp_perf_id`    BIGINT      NOT NULL AUTO_INCREMENT COMMENT '竞品表现ID，自增',
                                                   `comp_product_id` BIGINT      NOT NULL COMMENT '竞品商品ID',
                                                   `date_key`        DATE        NOT NULL COMMENT '关联日期维度',
                                                   `pv_count`        BIGINT      DEFAULT 0 COMMENT '竞品PV数',
                                                   `uv_count`        BIGINT      DEFAULT 0 COMMENT '竞品UV数',
                                                   `order_count`     BIGINT      DEFAULT 0 COMMENT '竞品订单数',
                                                   `sales_amount`    DECIMAL(12,2) DEFAULT 0 COMMENT '竞品销售额',
                                                   `rating_average`  DECIMAL(3,2) DEFAULT 0 COMMENT '竞品平均评分',
                                                   `price_change_flag` ENUM('Y','N') DEFAULT 'N' COMMENT '价格变动标志',
                                                   `create_time`     DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                                                   PRIMARY KEY (`comp_perf_id`),
                                                   KEY `idx_cp_prod` (`comp_product_id`),
                                                   KEY `idx_cp_date` (`date_key`),
                                                   CONSTRAINT `fk_cp_prod` FOREIGN KEY (`comp_product_id`) REFERENCES `ods_dim_competitor_product`(`comp_product_id`),
                                                   CONSTRAINT `fk_cp_date` FOREIGN KEY (`date_key`) REFERENCES `ods_dim_date`(`date_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='竞品关键表现表';



-- ================================================
-- 1. 准备：关闭外键检查，清空所有表
-- ================================================
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE ods_dim_date;
TRUNCATE TABLE ods_dim_user;
TRUNCATE TABLE ods_dim_product;
TRUNCATE TABLE ods_dim_sku;
TRUNCATE TABLE ods_dim_vendor;
TRUNCATE TABLE ods_dim_competitor_product;
TRUNCATE TABLE ods_fact_pageview;
TRUNCATE TABLE ods_fact_uv;
TRUNCATE TABLE ods_fact_pv_source;
TRUNCATE TABLE ods_fact_cart_add;
TRUNCATE TABLE ods_fact_order;
TRUNCATE TABLE ods_fact_order_item;
TRUNCATE TABLE ods_fact_payment;
TRUNCATE TABLE ods_fact_return;
TRUNCATE TABLE ods_fact_product_review;
TRUNCATE TABLE ods_fact_content_engagement;
TRUNCATE TABLE ods_fact_promotion_event;
TRUNCATE TABLE ods_fact_user_register;
TRUNCATE TABLE ods_fact_referral;
TRUNCATE TABLE ods_fact_service_ticket;
TRUNCATE TABLE ods_fact_after_sales;
TRUNCATE TABLE ods_fact_competitor_performance;
SET FOREIGN_KEY_CHECKS = 1;



