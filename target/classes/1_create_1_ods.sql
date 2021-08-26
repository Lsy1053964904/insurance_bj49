--开启spark-sql客户端，将下面的代码粘贴到spark-sql中运行。
drop database if exists insurance_ods cascade;
create database insurance_ods;
use insurance_ods;

drop  table if exists mort_10_13;
create table mort_10_13(
    age  smallint comment '年龄',
    cl1 decimal(10, 8) comment '非养老类业务一表，男（CL1）',
    cl2 decimal(10, 8) comment '非养老类业务一表，女（CL2）',
    cl3 decimal(10, 8) comment '非养老类业务二表，男（CL3）',
    cl4 decimal(10, 8) comment '非养老类业务二表，女（CL4）',
    cl5  decimal(10, 8) comment '养老类业务表，男（CL5）',
    cl6  decimal(10, 8) comment '养老类业务表，女（CL6）'
) comment '中国人身保险业经验生命表（2010－2013）'
row format delimited fields terminated by '\t';
load data local inpath '/opt/insurance/2_data_hive/mort_10_13.txt' overwrite into table mort_10_13;

drop table if exists dd_table;
create table dd_table(
    age      smallint comment '年龄',
    male     decimal(10, 8) comment '男性的重疾发生率',
    female   decimal(10, 8) comment '女性的重疾发生率',
    k_male   decimal(10, 8) comment '男性的K值',
    k_female decimal(10, 8) comment '女性的K值'
) comment '行业25种重疾发生率'
    row format delimited fields terminated by '\t';
load data local inpath '/opt/insurance/2_data_hive/dd_table.txt' overwrite into table dd_table;

--ASSUMPTION 预定附加费用率 pre_add_exp_ratio
drop table if exists  pre_add_exp_ratio;
create table pre_add_exp_ratio  (
                                    PPP smallint comment '缴费期',
                                    r1 decimal(10,8) comment '如果保单年度=1',
                                    r2 decimal(10,8) comment '如果保单年度=2',
                                    r3 decimal(10,8) comment '如果保单年度=3',
                                    r4 decimal(10,8) comment '如果保单年度=4',
                                    r5 decimal(10,8) comment '如果保单年度=5',
                                    r6_ decimal(10,8) comment '如果保单年度>=6',
                                    r_avg decimal(10,8) comment 'Avg',
                                    r_max decimal(10,8) comment '上限'
) comment '预定附加费用率'
 row format delimited fields terminated by '\t';
load data local inpath '/opt/insurance/2_data_hive/pre_add_exp_ratio.txt' overwrite into table pre_add_exp_ratio;


drop table if exists prem_std_real;
create table prem_std_real
(
    age_buy smallint comment '年投保龄',
    sex     string comment '性别',
    ppp     smallint comment '缴费期',
    bpp     string comment '保障期',
    prem    decimal(14, 6) comment '每期交的保费',
    nbev    decimal(10,8) comment '新业务价值率（NBEV，New Business Embed Value）'
)comment '标准保费真实参照表' row format delimited fields terminated by '\t';
load data local inpath '/opt/insurance/2_data_hive/prem_std_real.txt' overwrite into table prem_std_real;

drop table if exists prem_cv_real;
create table prem_cv_real
(
    age_buy smallint comment '年投保龄',
    sex     string comment '性别',
    ppp     smallint comment '缴费期间',
    prem_cv      decimal(15, 7) comment '保单价值准备金毛保险费(Preuim)'
)comment '保单价值准备金毛保险费，真实参照表'
    row format delimited fields terminated by '\t';
load data local inpath '/opt/insurance/2_data_hive/prem_cv_real.txt' overwrite into table prem_cv_real;

drop table if exists area;
create table area
(
    id        smallint comment '编号',
    province  string comment '省份',
    city      string comment '城市',
    direction String comment '大区域'
) comment '中国省市区域表' row format delimited fields terminated by '\t';
load data local inpath '/opt/insurance/2_data_hive/area.txt' overwrite into table area;


drop table if exists policy_client;
CREATE TABLE policy_client(
                              user_id STRING COMMENT '用户号',
                              name STRING COMMENT '姓名',
                              id_card STRING COMMENT '身份证号',
                              phone STRING COMMENT '手机号',
                              sex STRING COMMENT '性别',
                              birthday STRING COMMENT '出生日期',
                              province STRING COMMENT '省份',
                              city STRING COMMENT '城市',
                              direction STRING COMMENT '区域',
                              income INT COMMENT '收入'
)
    comment '客户信息表' row format delimited fields terminated by '\t';
load data local inpath '/opt/insurance/2_data_hive/policy_client.txt' overwrite into table policy_client;


drop table if exists policy_benefit;
CREATE TABLE policy_benefit(  pol_no STRING COMMENT '保单号',
                              user_id STRING COMMENT '用户号',
                              ppp STRING COMMENT '缴费期',
                              age_buy BIGINT COMMENT '投保年龄',
                              buy_datetime STRING COMMENT '购买日期',
                              insur_name STRING COMMENT '保险名称',
                              insur_code STRING COMMENT '保险代码',
                              pol_flag smallint COMMENT '保单状态，1有效，0失效',
                              elapse_date STRING COMMENT '保单失效时间')
    comment '客户投保详情表' row format delimited fields terminated by '\t';
load data local inpath '/opt/insurance/2_data_hive/policy_benefit.txt' overwrite into table policy_benefit;

drop table if exists claim_info;
create table claim_info
(
    pol_no string comment '保单号',
    user_id string comment '用户号',
    buy_datetime string comment '购买日期',
    insur_code string comment '保险代码',
    claim_date string comment '理赔日期',
    claim_item string comment '理赔责任',
    claim_mnt decimal(35,6) comment '理赔金额'
)  comment '理赔信息表'
    row format delimited fields terminated by '\t';
load data local inpath '/opt/insurance/2_data_hive/claim_info.txt' overwrite into table claim_info;

drop table if exists policy_surrender;
create table  policy_surrender
(
    pol_no string comment '保单号',
    user_id string comment '用户号',
    buy_datetime string comment '投保日期',
    keep_days smallint comment '退保前的保单持有天数',
    elapse_date string comment '保单失效日期'
) comment '退保记录表'
    row format delimited fields terminated by '\t';
load data local inpath '/opt/insurance/2_data_hive/policy_surrender.txt' overwrite into table policy_surrender;
