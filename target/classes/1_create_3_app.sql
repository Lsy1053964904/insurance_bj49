--开启spark-sql客户端，将下面的代码粘贴到spark-sql中运行。
drop database if exists insurance_app cascade;
create database insurance_app;
use insurance_app;
drop table if exists insurance_app.policy_actuary;
create table insurance_app.policy_actuary
(
    age_buy     smallint comment '年投保龄',
    sex         string comment '性别',
    ppp         smallint comment '交费期间(Premuim Payment Period PPP)',
    bpp         smallint comment '保险期间(BPP)',
    policy_year smallint comment '保单年度',
    sa          decimal(12, 2) comment '基本保险金额(Baisc Sum Assured)',
    cv_1a       decimal(17, 7) comment '现金价值年末（生存给付前）',
    cv_1b       decimal(17, 7) comment '现金价值年末（生存给付后）',
    sur_ben     decimal(17, 7) comment '生存金',
    np          decimal(17, 7) comment '修匀净保费',
    rsv2_re     decimal(17, 7) comment '修正责任准备金年初(未加当年初纯保费）',
    rsv1_re     decimal(17) comment '修正责任准备金年末',
    np_         decimal(12) comment '修正纯保费'
) comment '产品精算数据表' row format delimited fields terminated by '\t';


drop table if exists policy_result;
create table policy_result
(
    pol_no         STRING COMMENT '保单号',
    user_id        string comment '客户id',
    name           string comment '姓名',
    sex            string comment '性别',
    birthday       string comment '出生日期',
    ppp            string comment '缴费期',
    age_buy        bigint comment '投保年龄',
    buy_datetime   string comment '投保日期',
    insur_name     STRING COMMENT '保险名称',
    insur_code     STRING COMMENT '保险代码',
    province       string comment '所在省份',
    city           string comment '所在城市',
    direction      String comment '所在区域',
    bpp            smallint comment '保险期间，保障期',
    policy_year    smallint comment '保单年度',
    sa             decimal(12, 2) comment '保单年度基本保额',
    cv_1a          decimal(17, 7) comment '现金价值给付前',
    cv_1b          decimal(17, 7) comment '现金价值给付后',
    sur_ben        decimal(17, 7) comment '生存给付金',
    np             decimal(17, 7) comment '纯保费（CV.NP）',
    rsv2_re        decimal(17, 7) comment '年初责任准备金',
    rsv1_re        decimal(17, 7) comment '年末责任准备金',
    np_            decimal(12, 2) comment '纯保费(RSV.np_) ',
    prem_std       decimal(14, 6) comment '每期交保费',
    prem_thismonth decimal(14, 6) comment '本月应交保费'
) partitioned by (month string)
    comment '客户保单精算结果表' row format delimited fields terminated by '\t';

--保费收入增长率
drop table if exists app_agg_month_incre_rate;
CREATE TABLE app_agg_month_incre_rate
(
    prem            DECIMAL(24, 6) comment '本月保费收入',
    last_prem       DECIMAL(24, 6) comment '上月保费收入',
    prem_incre_rate DECIMAL(8, 6) comment '保费收入增长率'
) partitioned by (month string comment '月份')
    comment '保费收入增长率表' row format delimited fields terminated by '\t';



--首年保费与保费收入比
drop TABLE if exists app_agg_month_first_of_total_prem;
CREATE TABLE app_agg_month_first_of_total_prem
(
    first_prem          DECIMAL(24, 6),
    total_prem          DECIMAL(24, 6),
    first_of_total_prem DECIMAL(8, 6)
) partitioned by (month string comment '月份')
    comment '首年保费与保费收入比表' row format delimited fields terminated by '\t';

--13个月续保率 13 month persistence ratio
drop table if exists app_agg_month_rate_eff13;
create table app_agg_month_rate_eff13
(
    insur_code string comment '保险代码',
    insur_name string comment '保险名称',
    rate_eff13 decimal(8, 6) comment '13个月续保率'
) partitioned by (month string comment '月份')
    comment '13个月续保率表' row format delimited fields terminated by '\t';

--个人营销渠道的件均保费
drop TABLE if exists app_agg_month_premperpol;
CREATE TABLE app_agg_month_premperpol
(
    insur_code   string comment '保险代码',
    insur_name   string comment '保险名称',
    prem_per_pol DECIMAL(38, 2) comment '个人营销渠道的件均保费'
) partitioned by (month string comment '月份')
    comment '个人营销渠道的件均保费' row format delimited fields terminated by '\t';

--犹豫期保费退保率
drop TABLE if exists app_agg_month_cooloff_surr;
CREATE TABLE app_agg_month_cooloff_surr
(
    insur_code   string comment '保险代码',
    insur_name   string comment '保险名称',
    cooloff      DECIMAL(26, 6) comment '犹豫期撤单保费',
    cnt_all      DECIMAL(24, 6) comment '总的新单原保费收入',
    cooloff_rate DECIMAL(8, 6) comment '犹豫期保费退保率'
) partitioned by (month string comment '月份')
    comment '犹豫期保费退保率' row format delimited fields terminated by '\t';

--退保率 surrender rate
drop table if exists app_agg_month_surr_rate;
create table app_agg_month_surr_rate
(
    insur_code string comment '保险代码',
    insur_name string comment '保险名称',
    cnt_surr   DECIMAL(26, 6) comment '退保人数',
    cnt_all    DECIMAL(24, 6) comment '总的有效保单数',
    surr_rate  decimal(8, 6) comment '退保率'
) partitioned by (month string comment '月份')
    comment '退保率表' row format delimited fields terminated by '\t';
--死亡发生率和残疾发生率表
DROP TABLE if exists app_agg_month_mort_dis_rate;
CREATE TABLE app_agg_month_mort_dis_rate
(
    insur_code string comment '保险代码',
    insur_name string comment '保险名称',
    age        int comment '年龄',
    sg_cnt     int comment '发生身故的保单数',
    sc_cnt     int comment '发生身残的保单数',
    all_cnt    int comment '所有有效保单数',
    sg_rate    decimal(8, 6) comment '死亡发生率',
    sc_rate    decimal(8, 6) comment '残疾发生率'
) partitioned by (month string comment '月份')
    comment '死亡发生率和残疾发生率表' row format delimited fields terminated by '\t';

--统计分析  计算全国各区域的投保人数，和保费汇总，并按照投保人数倒序排序。
drop table if exists app_agg_month_dir;
create table app_agg_month_dir
(
    direction   string comment '所在区域',
    sum_users   bigint comment '总投保人数',
    sum_prem    decimal(24) comment '当月保费汇总',
    sum_cv_1b   decimal(27, 2) comment '总现金价值',
    sum_sur_ben decimal(27) comment '总生存金',
    sum_rsv2_re decimal(27, 2) comment '总准备金'
) partitioned by (month string comment '月份')
    comment '各地区的汇总保费表' row format delimited fields terminated by '\t';

--新业务价值率
drop table if exists app_agg_month_nbev;
create table app_agg_month_nbev
(
    insur_code string comment '保险代码',
    insur_name string comment '保险名称',
    nbev       decimal(38, 11) comment '新业务价值率'
) partitioned by (month string comment '月份')
    comment '新业务价值率表' row format delimited fields terminated by '\t';

--高净值客户比例
drop table if exists app_agg_month_high_net_rate;
create table app_agg_month_high_net_rate(
    high_net_rate decimal(8, 6) comment '高净值客户比例'
) partitioned by (month string comment '月份') comment '高净值客户比例表' row format delimited fields terminated by '\t';



