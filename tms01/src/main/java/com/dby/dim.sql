set hive.exec.mode.local.auto=True;
use tms01;

drop table if exists dim_complex_full;
create external table if not exists dim_complex_full(
       `id` string COMMENT '小区ID',
       `complex_name` string COMMENT '小区名称',
       `courier_emp_ids` array<string> COMMENT '负责快递员IDS',
       `province_id` string COMMENT '省份ID',
       `province_name` string COMMENT '省份名称',
       `city_id` string comment '城市ID',
       `city_name` string COMMENT '城市名称',
       `district_id` string comment '区（县）ID',
       `district_name` string comment '区（县）名称'
) comment '小区维度表'
    partitioned by (`ds` string comment '统计日期')
    stored as orc
    location '/bigdata_warehouse/tms01/dim/dim_complex_full'
    tblproperties ('orc.compress'='snappy');


with cx as (
    select id,
           complex_name,
           province_id,
           city_id,
           district_id,
           district_name
    from ods_base_complex where ds='20250717' and is_deleted='0'
),pv as (
    select id,
           name
    from ods_base_region_info where ds='20250717' and is_deleted='0'
),cy as (
    select id,
           name
    from ods_base_region_info where ds='20250717' and is_deleted='0'
),ex as (
    select collect_set(cast(courier_emp_id as string)) courier_emp_ids,
           complex_id
    from ods_express_courier_complex where ds='20250717' and is_deleted='0'
    group by complex_id
)
insert overwrite table dim_complex_full partition (ds='20250717')
select
    cx.id,
    complex_name,
    courier_emp_ids,
    province_id,
    pv.name,
    city_id,
    cy.name,
    district_id,
    district_name
from cx left join pv
                  on cx.province_id=pv.id
        left join cy
                  on cx.city_id=cy.id
        left join ex
                  on cx.id=ex.complex_id;
select * from dim_complex_full;


drop table if exists dim_organ_full;
create external table dim_organ_full(
    `id` bigint COMMENT '机构ID',
    `org_name` string COMMENT '机构名称',
    `org_level` bigint COMMENT '机构等级（1为转运中心，2为转运站）',
    `region_id` bigint COMMENT '地区ID，1级机构为city ,2级机构为district',
    `region_name` string COMMENT '地区名称',
    `region_code` string COMMENT '地区编码（行政级别）',
    `org_parent_id` bigint COMMENT '父级机构ID',
    `org_parent_name` string COMMENT '父级机构名称'
) comment '机构维度表'
    partitioned by (`ds` string comment '统计日期')
    stored as orc
    location '/bigdata_warehouse/tms01/dim/dim_organ_full'
    tblproperties ('orc.compress'='snappy');


insert overwrite table dim_organ_full
    partition (ds = '20250717')
select organ_info.id,
       organ_info.org_name,
       org_level,
       region_id,
       region_info.name        region_name,
       region_info.dict_code   region_code,
       org_parent_id,
       org_for_parent.org_name org_parent_name
from (select id,
             org_name,
             org_level,
             region_id,
             org_parent_id
      from ods_base_organ
      where ds = '20250717'
        and is_deleted = '0') organ_info
         left join (
    select id,
           name,
           dict_code
    from ods_base_region_info
    where ds = '20250717'
      and is_deleted = '0'
) region_info
                   on organ_info.region_id = region_info.id
         left join (
    select id,
           org_name
    from ods_base_organ
    where ds = '20250717'
      and is_deleted = '0'
) org_for_parent
                   on organ_info.org_parent_id = org_for_parent.id;
select * from dim_organ_full;

drop table dim_region_full;
create external table if not exists dim_region_full(
    `id` string COMMENT 'id',
    `parent_id` string COMMENT '上级id',
    `name` string COMMENT '名称',
    `dict_code` string COMMENT '编码',
    `short_name` string COMMENT '简称'
)comment '地区维度表'
    partitioned by (ds string comment '统计日期')
    stored as orc
    location '/bigdata_warehouse/tms01/dim/dim_region_full'
    tblproperties ('orc.compress'='snappy');

insert overwrite table dim_region_full partition (ds='20250717')
select
    id,
    parent_id,
    name,
    dict_code,
    short_name
from ods_base_region_info where ds='20250717' and is_deleted='0';
select * from dim_region_full;


create external table if not exists dim_express_courier_full(
    `id` string COMMENT '快递员id',
    `emp_id` string COMMENT '员工id',
    `org_id` string COMMENT '所属机构ID',
    `org_name` string COMMENT '机构名称',
    `working_phone` string COMMENT '工作电话',
    `express_type` string COMMENT '快递员类型（收货，发货）',
    `express_type_name` string COMMENT '快递员类型名称'
)comment '快递员维度表'
    partitioned by (ds string comment '统计日期')
    stored as orc
    location '/bigdata_warehouse/tms01/dim/dim_express_courier_full'
    tblproperties ('orc.compress'='snappy');

with t1 as (
    select
        id,
        emp_id,
        org_id,
        working_phone,
        express_type
    from ods_express_courier where ds='20250717' and is_deleted='0'
),t2 as (
    select
        id,
        org_name
    from ods_base_organ where ds='20250717' and is_deleted='0'
),t3 as (
    select
        id,
        name
    from ods_base_dic where ds='20250717' and is_deleted='0'
)
insert overwrite table dim_express_courier_full partition (ds='20250717'
    '')
select
    t1.id,
    emp_id,
    org_id,
    t2.org_name,
    working_phone,
    express_type,
    t3.name
from t1 left join t2
                  on t1.org_id=t2.id
        left join t3
                  on t1.express_type=t3.id;

select * from dim_express_courier_full;

drop table dim_shift_full;
create external  table dim_shift_full(
    `id` bigint comment '班次ID',
    `line_id` bigint COMMENT '线路ID',
    `line_name` string comment '线路名称',
    `line_no` string comment '线路编号',
    `line_level` string COMMENT'线路级别',
    `org_id` bigint COMMENT '所属机构',
    `transport_line_type_id`string COMMENT '线路类型ID',
    `transport_line_type_name` string comment '线路类型名称',
    `start_org_id` bigint COMMENT '起始机构ID',
    `start_org_name`string COMMENT '起始机构名称',
    `end_org_id`bigint COMMENT'目标机构ID',
    `end_org_name` string COMMENT'目标机构名称',
    `pair_line_id`bigint COMMENT '配对线路ID',
    `distance`decimal(10,2)COMMENT '直线距离',
    `cost`decimal(10,2)COMMENT '公路里程',
    `estimated_time` bigint COMMENT '预计时间(分钟)',
    `start_time` string COMMENT'班次开始时间',
    `driver1_emp_id` bigint COMMENT '第一司机',
    `driver2_emp_id` bigint COMMENT '第二司机',
    `truck_id` bigint COMMENT '卡车ID',
    `pair_shift_id` bigint COMMENT'配对班次（同一辆车一去一回的另一班次)'
)comment '班次维度表'
    partitioned by (`ds` string comment '统计周期')
    stored as orc
    location '/bigdata_warehouse/tms01/dim/dim_shift_full'
    tblproperties ('orc.compress' = 'snappy');


with t1 as (
    select
        id,
        line_id,
        start_time,
        driver1_emp_id,
        driver2_emp_id,
        truck_id,
        pair_shift_id
    from ods_line_base_shift where ds='20250717' and is_deleted='0'
),t2 as (
    select
        id,
        name,
        line_no,
        line_level,
        org_id,
        transport_line_type_id,
        start_org_id,
        start_org_name,
        end_org_id,
        end_org_name,
        pair_line_id,
        distance,
        `cost`,
        estimated_time,
        `status`
    from ods_line_base_info where ds='20250717' and is_deleted='0'
),t3 as (
    select
        id,
        name
    from ods_base_dic where ds='20250717' and is_deleted='0'
)
insert overwrite table dim_shift_full partition (ds='20250717')
select
    t1.id,
    line_id,
    t2.name,
    line_no,
    line_level,
    org_id,
    transport_line_type_id,
    t3.name,
    start_org_id,
    start_org_name,
    end_org_id,
    end_org_name,
    pair_shift_id,
    distance,
    `cost`,
    estimated_time,
    start_time,
    driver1_emp_id,
    driver2_emp_id,
    truck_id,
    pair_line_id
from t1 left join t2
                  on t1.line_id=t2.id
        left join t3
                  on t2.transport_line_type_id=t3.id;
select * from dim_shift_full;


create table if not exists dim_truck_driver_full(
                                                    `id` string comment '司机信息ID',
                                                    `emp_id` string COMMENT '雇员编号',
                                                    `org_id` string COMMENT '机构号',
                                                    `org_name` string COMMENT '机构名称',
                                                    `team_id` string COMMENT '车队号',
                                                    `team_name` string COMMENT '车队名称',
                                                    `license_type` string COMMENT '准驾车型',
                                                    `init_license_date` string COMMENT '初次领证日期',
                                                    `expire_date` string COMMENT '有效截止日期',
                                                    `license_no` string COMMENT '驾驶证号',
                                                    `is_deleted` string COMMENT '删除标记（0:不可用 1:可用）'
)comment '司机维度表'
    partitioned by (ds string comment '统计日期')
    stored as orc
    location '/bigdata_warehouse/tms01/dim/dim_truck_driver_full'
    tblproperties ('orc.compress'='snappy');

with t1 as (
    select
        id,
        emp_id,
        org_id,
        team_id,
        license_type,
        init_license_date,
        expire_date,
        license_no,
        license_picture_url,
        is_deleted
    from ods_truck_driver where ds='20250717' and is_deleted='0'
),t2 as (
    select
        id,
        name
    from ods_truck_team where ds='20250717' and is_deleted='0'
),t3 as (
    select
        id,
        org_name
    from ods_base_organ where ds='20250717' and is_deleted='0'
)
insert overwrite table dim_truck_driver_full partition (ds='20250717')
select
    t1.id,
    emp_id,
    org_id,
    org_name,
    team_id,
    t2.name,
    license_type,
    init_license_date,
    expire_date,
    license_no,
    is_deleted
from t1 left join t2
                  on t1.team_id=t2.id
        left join t3
                  on t1.org_id=t3.id;
select * from dim_truck_driver_full;


create external table if not exists dim_truck_full(
    `id` string COMMENT '卡车id',
    `team_id` string COMMENT '所属车队id',
    `team_name` string COMMENT '车队名称',
    `team_no` string COMMENT '车队编号',
    `org_id` string COMMENT '所属机构',
    `org_name` string COMMENT '机构名称',
    `manager_emp_id` string COMMENT '负责人',
    `truck_no` string COMMENT '车牌号码',
    `truck_model_id` string COMMENT '型号',
    `truck_model_name` string COMMENT '型号名称',
    `truck_model_type` string COMMENT '类型',
    `truck_model_type_name` string COMMENT '类型名称',
    `truck_model_no` string COMMENT '型号编码',
    `truck_brand` string COMMENT '品牌',
    `truck_brand_name` string COMMENT '品牌名称',
    `truck_weight` decimal(10,2) COMMENT '整车重量 吨',
    `load_weight` decimal(10,2) COMMENT '额定载重 吨',
    `total_weight` decimal(10,2) COMMENT '总质量 吨',
    `eev` string COMMENT '排放标准',
    `boxcar_len` decimal(10,2) COMMENT '货箱长m',
    `boxcar_wd` decimal(10,2) COMMENT '货箱宽m',
    `boxcar_hg` decimal(10,2) COMMENT '货箱高m',
    `max_speed` string COMMENT '最高时速 千米每时',
    `oil_vol` string COMMENT '油箱容积 升',
    `device_gps_id` string COMMENT 'GPS设备id',
    `engine_no` string COMMENT '发动机编码',
    `license_registration_date` string COMMENT '注册时间',
    `license_last_check_date` string COMMENT '最后年检日期',
    `license_expire_date` string COMMENT '失效事件日期',
    `is_enabled` string COMMENT '状态 0：禁用 1：正常'
)comment '卡车维度表'
    partitioned by (ds string comment '统计日期')
    stored as orc
    location '/bigdata_warehouse/tms01/dim/dim_truck_full'
    tblproperties ('orc.compress'='snappy');

with tk as (
    select
        id,
        team_id,
        truck_no,
        truck_model_id,
        device_gps_id,
        engine_no,
        license_registration_date,
        license_last_check_date,
        license_expire_date,
        picture_url,
        is_enabled
    from ods_truck_info where ds='20250717' and is_deleted='0'
),tm as (
    select
        id,
        name,
        team_no,
        org_id,
        manager_emp_id
    from ods_truck_team where ds='20250717' and is_deleted='0'
),og as (
    select
        id,
        org_name
    from ods_base_organ where ds='20250717' and is_deleted='0'
),bc as (
    select
        id,
        name
    from ods_base_dic where ds='20250717' and is_deleted='0'
),td as (
    select
        id,
        model_name,
        model_type,
        model_no,
        brand,
        truck_weight,
        load_weight,
        total_weight,
        eev,
        boxcar_len,
        boxcar_wd,
        boxcar_hg,
        max_speed,
        oil_vol
    from ods_truck_model where ds='20250717' and is_deleted='0'
)
insert overwrite table dim_truck_full partition (ds='20250717')
select
    tk.id,
    team_id,
    tm.name,
    team_no,
    org_id,
    org_name,
    manager_emp_id,
    truck_no,
    truck_model_id,
    td.model_name,
    model_type,
    mtp.name,
    model_no,
    brand,
    bd.name,
    truck_weight,
    load_weight,
    total_weight,
    eev,
    boxcar_len,
    boxcar_wd,
    boxcar_hg,
    max_speed,
    oil_vol,
    device_gps_id,
    engine_no,
    license_registration_date,
    license_last_check_date,
    license_expire_date,
    is_enabled
from tk left join tm
                  on tk.team_id=tm.id
        left join og
                  on tm.org_id=og.id
        left join td
                  on tk.truck_model_id=td.id
        left join bc mtp
                  on td.model_type=mtp.id
        left join bc bd
                  on td.brand=bd.id;
select * from dim_truck_full;

drop table dim_user_zip;
create external table dim_user_zip(
    `id` bigint comment '用户地址信息ID',
    `login_name` string comment '用户名称',
    `nick_name` string comment '用户昵称',
    `passwd` string comment '用户密码',
    `real_name` string comment '用户姓名',
    `phone_num` string comment '手机号',
    `email` string comment '邮箱',
    head_img string comment '头像',
    `user_level` string comment '用户性别',
    `birthday` string comment '用户生日',
    `gender` string COMMENT'性别M男,F女',
    `start_date`string COMMENT '起始日期',
    `end_date`string COMMENT '结束日期'

)comment '用户拉链表'
    partitioned by (`ds` string comment '统计日期')
    stored as orc
    location '/bigdata_warehouse/tms01/dim/dim_user_zip'
    tblproperties ('orc.compress'='snappy');

insert overwrite table dim_user_zip partition (ds='99991231')
select
    id,
    login_name,
    nick_name,
    passwd,
    real_name,
    phone_num,
    email,
    head_img,
    user_level,
    birthday,
    gender,
    date_format(nvl(create_time,update_time),'yyyy-MM-dd'),
    '9999-12-31'
from ods_user_info where ds='20250717' and is_deleted='0';
select * from dim_user_zip;

SET hive.exec.dynamic.partition.mode=nonstrict;
with old as (
    select
        cast(id as string) id,  -- 将id转换为string类型
        login_name,
        nick_name,
        passwd,
        real_name,
        phone_num,
        email,
        head_img,
        user_level,
        birthday,
        gender,
        start_date,
        end_date
    from dim_user_zip where ds='99991231'
),
     new as (
         select
             cast(id as string) id,  -- 将id转换为string类型
             login_name,
             nick_name,
             passwd,
             real_name,
             phone_num,
             email,
             head_img,
             user_level,
             birthday,
             gender,
             '20250715' start_date,
             '99991231' end_date
         from (
                  select
                      id,
                      login_name,
                      nick_name,
                      passwd,
                      real_name,
                      phone_num,
                      email,
                      head_img,
                      user_level,
                      birthday,
                      gender,
                      row_number() over (partition by id order by update_time desc) rn
                  from ods_user_info where ds='20251231' and is_deleted='0'
              ) t1 where rn=1
     ),
     full_user as (
         select
             id,
             login_name,
             nick_name,
             passwd,
             real_name,
             phone_num,
             email,
             head_img,
             user_level,
             birthday,
             gender,
             start_date,
             end_date,
             row_number() over (partition by id order by start_date desc) rn
         from (
                  select
                      id,
                      login_name,
                      nick_name,
                      passwd,
                      real_name,
                      phone_num,
                      email,
                      head_img,
                      user_level,
                      birthday,
                      gender,
                      start_date,
                      end_date
                  from new
                  union all
                  select
                      id,
                      login_name,
                      nick_name,
                      passwd,
                      real_name,
                      phone_num,
                      email,
                      head_img,
                      user_level,
                      birthday,
                      gender,
                      start_date,
                      end_date
                  from old
              ) t1
     )
insert overwrite table dim_user_zip partition (ds='20250715')
select
    id,
    login_name,
    nick_name,
    passwd,
    real_name,
    phone_num,
    email,
    head_img,
    user_level,
    birthday,
    gender,
    start_date,
    end_date
from full_user where rn=1
union all
select
    id,
    login_name,
    nick_name,
    passwd,
    real_name,
    phone_num,
    email,
    head_img,
    user_level,
    birthday,
    gender,
    start_date,
    cast(date_sub('20250715',1) as string) as end_date
from full_user where rn!=1;


create external table if not exists dim_user_address_zip(
    `id` string COMMENT 'id',
    `user_id` string COMMENT '用户id',
    `phone` string COMMENT '电话号',
    `province_id` string COMMENT '所属省份id',
    `city_id` string COMMENT '所属城市id',
    `district_id` string COMMENT '所属区县id',
    `complex_id` string COMMENT '所属小区id',
    `address` string COMMENT '详细地址',
    `is_default` string COMMENT '是否默认 0:否，1:是',
    `start_date` string comment '起始日期',
    `end_date` string comment '结束日期'
)comment '用户地址拉链表'
    partitioned by (ds string comment '统计日期')
    stored as orc
    location '/bigdata_warehouse/tms01/dim/dim_user_address_zip'
    tblproperties ('orc.compress'='snappy');
-- 1
----首日
insert overwrite table dim_user_address_zip partition (ds='99991231')
select
    id,
    user_id,
    phone,
    province_id,
    city_id,
    district_id,
    complex_id,
    address,
    is_default,
    date_format(nvl(update_time,create_time),'yyyy-MM-dd'),
    '99991231'
from ods_user_address where ds='20250714' and is_deleted='0';







