use insurance_app;
set spark.sql.shuffle.partitions=8;
--计算产品精算数据表
cache table policy_actuary_cache as
select a.age_buy,
       a.sex,
       a.ppp,
       a.bpp,
       a.policy_year,
       a.sa,
       a.cv_1a,
       a.cv_1b,
       a.sur_ben,
       a.np,
       b.rsv2_re,
       b.rsv1_re,
       b.np_
from insurance_dw.cv_src a
join insurance_dw.rsv_src b
on a.age_buy=b.age_buy
and a.sex=b.sex
and a.ppp=b.ppp
and a.policy_year=b.policy_year;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from policy_actuary_cache
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;
--缓存数据校验通过后，保存至hive表中
insert overwrite table insurance_app.policy_actuary
select * from policy_actuary_cache;

--指定当前的报表的统计月份
set calc_month = '2021-04';
select ${calc_month} as calc_month;
select ceil(months_between(${calc_month},'2020-03-01')/12) as m_bet;
select substr('2021-04',6,2) as this_month;
select substr('2018-04-12',6,2) as this_month;
select substr('2018-04-12',0,7) as year_month;

--计算用户当月保单精算数据表
--向分区表动态插入数据。
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
--insert overwrite table insurance_app.policy_result partition (month)
insert overwrite table insurance_app.policy_result partition (month=${calc_month})
select a.pol_no,
       a.user_id,
       b.name,
       b.sex,
       b.birthday,
       a.ppp,
       a.age_buy,
       a.buy_datetime,
       a.insur_name,
       a.insur_code,
       b.province,
       b.city,
       b.direction,
       c.bpp,
       ceil(months_between(${calc_month},a.buy_datetime)/12) as policy_year,
       c.sa,
       c.cv_1a,
       c.cv_1b,
       c.sur_ben,
       c.np,--现金价值相关的纯保费
       c.rsv2_re,
       c.rsv1_re,
       c.np_, --准备金相关额纯保费
       d.prem as prem_std,
       --仍在缴费期内
       --且本月份是首次缴费的月份，即刚好经过了整数年 2021-04   2018-04-12
       if( ceil(months_between(${calc_month},a.buy_datetime)/12)<=a.ppp
               and substr(${calc_month},6,2)=substr(a.buy_datetime,6,2),
           d.prem,0) as prem_thismonth--, --本月应交保费
       --${calc_month} as month
from insurance_ods.policy_client b
join insurance_ods.policy_benefit a
     on a.user_id=b.user_id
left join insurance_app.policy_actuary c
          on a.age_buy=c.age_buy
              and b.sex=c.sex
              and a.ppp=c.ppp
              and ceil(months_between(${calc_month},a.buy_datetime)/12)=c.policy_year
left join insurance_ods.prem_std_real d
          on d.age_buy=a.age_buy
              and d.sex=b.sex
              and d.ppp=a.ppp
where substr(a.buy_datetime,0,7)<=${calc_month}
;

--查看表有几个分区
show partitions insurance_app.policy_result;
--查看2021-04的数目
select count(1) from insurance_app.policy_result where month='2021-04';

--计算保费收入增长率
--Spark 2.x版本中默认不支持笛卡尔积操作，需要手动开启
set spark.sql.crossJoin.enabled=true;
select substr(add_months(${calc_month},-1),0,7) as new_month;
insert overwrite table insurance_app.app_agg_month_incre_rate partition (month=${calc_month})
select prem,last_prem,(prem-last_prem)/last_prem as prem_incre_rate from
    (select sum(prem_thismonth) prem  from insurance_app.policy_result where month=${calc_month} and prem_thismonth>0) a join
    (select sum(prem_thismonth) last_prem from insurance_app.policy_result where month=substr(add_months(${calc_month},-1),0,7) and prem_thismonth>0) b
    on 1=1;

select * from insurance_app.app_agg_month_incre_rate where month='2021-04';

--计算首年保费与保费收入比
create temporary view t1 as
select b.prem                                                                   as first_prem, --就是每个保单的首年保费
       --已经收取的所有保费
       least(ceil(months_between(${calc_month}, a.buy_datetime) / 12), a.ppp,
             ceil(months_between(a.elapse_date, a.buy_datetime) / 12)) * b.prem as totol_prem
from insurance_ods.policy_benefit a
join insurance_ods.policy_client  c on a.user_id = c.user_id
join insurance_ods.prem_std_real  b on b.age_buy = a.age_buy and b.sex = c.sex and b.ppp = a.ppp;
insert overwrite  table insurance_app.app_agg_month_first_of_total_prem partition (month = ${calc_month})
select sum(first_prem) as first_prem,
       sum(totol_prem) as totol_prem,
       sum(first_prem) / sum(totol_prem) as first_of_total_prem
from t1;
;

--13个月续保率 13 month persistence ratio
select substr(add_months(${calc_month},-13),0,7) as new_month;

create or replace temporary view t2  as
select insur_code,insur_name,
       sum(if(a.elapse_date > ${calc_month} or a.elapse_date is null, 1, 0)) as eff13_num,--分子，13月后依然有效的保单数
       count(a.pol_no)                                                       as total_eff_num --分母，13月前的所有有效保单数。
from insurance_ods.policy_benefit a
where a.buy_datetime <= substr(add_months(${calc_month}, -13), 0, 7)
  and (a.elapse_date > substr(add_months(${calc_month}, -13), 0, 7) or a.elapse_date is null)
group by insur_code,insur_name
;
insert overwrite table insurance_app.app_agg_month_rate_eff13 partition (month = ${calc_month})
select insur_code,insur_name, eff13_num / total_eff_num as rate_eff13
from t2;

--个人营销渠道的件均保费
insert overwrite table insurance_app.app_agg_month_premperpol partition (month = ${calc_month})
select insur_code,
       insur_name,
       ----分子（本月的）个人营销渠道的首年原保费总收入/分母 （本月的）个人营销渠道的新单总件数
       sum(b.prem) / count(a.pol_no) as prem_per_pol
from insurance_ods.policy_benefit a
join insurance_ods.policy_client  c on a.user_id = c.user_id
join insurance_ods.prem_std_real  b on b.age_buy = a.age_buy and b.sex = c.sex and b.ppp = a.ppp
where substr(buy_datetime, 0, 7) = ${calc_month}
group by insur_code, insur_name
;


--3.5 犹豫期保费退保率
--每月犹豫期保费退保率=每月的犹豫期撤单保费÷（每月的新单原保费收入+每月的犹豫期撤单保费）×100％
--20天内退保，算犹豫期
insert overwrite table insurance_app.app_agg_month_cooloff_surr partition (month = ${calc_month})
select a.insur_code,
       a.insur_name,
       sum(if(s.keep_days <= 20, r.prem, 0))               as cooloff,--分子 每月的犹豫期撤单保费
       sum(r.prem)                                         as cnt_all, --分母 每月的原本的所有的新单原保费收入
       sum(if(s.keep_days <= 20, r.prem, 0)) / sum(r.prem) as cooloff_rate --犹豫期保费退保率
from insurance_ods.policy_benefit        a
join      insurance_ods.policy_client    c on a.user_id = c.user_id
left join insurance_ods.policy_surrender s on a.pol_no = s.pol_no
join      insurance_ods.prem_std_real    r on r.age_buy = a.age_buy and r.sex = c.sex and r.ppp = a.ppp
where substr(a.buy_datetime, 0, 7) = ${calc_month}
group by a.insur_code, a.insur_name;


--退保率
select substr(add_months(${calc_month},-14),0,7) as new_month;
create temporary view t3 as
select a.insur_code,
       a.insur_name,
       sum(if(s.elapse_date > substr(add_months(${calc_month}, -14), 0, 7) and s.elapse_date < ${calc_month}, 1,
              0))      as cnt_surr,-- 分子：分母中的保单在近14个月内出现了退保的保单数
       count(a.pol_no) as cnt_all  -- 分母：前14个月前时刻的所有有效保单数。
from insurance_ods.policy_benefit        a
left join insurance_ods.policy_surrender s on a.pol_no = s.pol_no
where substr(add_months(${calc_month}, -14), 0, 7) >= substr(a.buy_datetime, 0, 7)
  and (substr(add_months(${calc_month}, -14), 0, 7) < a.elapse_date or a.elapse_date is null)
group by a.insur_code, a.insur_name ;

insert overwrite table insurance_app.app_agg_month_surr_rate partition (month=${calc_month})
select insur_code, insur_name, cnt_surr, cnt_all,
       cnt_surr/cnt_all as surr_rate
from t3;


--死亡发生率 =在月末时点，统计每个年龄的人群，按一岁一组，计算其中历史所有发生过死亡的保单数/所有的有效保单
--残疾发生率 =在月末时点，统计每个年龄的人群，按一岁一组，计算其中历史所有发生过残疾的保单数/所有的有效保单
create or replace temporary view t4 as
select a.insur_code,a.insur_name,
     --c.claim_date-b.birthday
     ceil(months_between(c.claim_date,b.birthday)/12) age, --理赔时的年龄
     sum(if(c.claim_item like '%sgbxj%',1,0)) as   sg_cnt,
     sum(if(c.claim_item like '%scbxj%',1,0)) as   sc_cnt,
     sum(count(1)) over () all_cnt
from insurance_ods.policy_benefit a
join insurance_ods.policy_client b on a.user_id=b.user_id
left join insurance_ods.claim_info c
on a.pol_no=c.pol_no
--当前月份的有效保单数
where a.elapse_date is null
group by a.insur_code,a.insur_name,age;
insert overwrite table insurance_app.app_agg_month_mort_dis_rate partition (month = ${calc_month})
select insur_code,
       insur_name,
       age,
       sg_cnt,
       sc_cnt,
       all_cnt,
       sg_cnt / all_cnt as sg_rate,
       sc_cnt / all_cnt as sc_rate
from t4;

--5.3 各地区的汇总保费
insert overwrite table insurance_app.app_agg_month_dir partition (month=${calc_month})
select direction,
       count(user_id)      as sum_users,
       sum(prem_thismonth) as sum_prem,
       sum(cv_1b)          as sum_cv_1b,
       sum(sur_ben)        as sum_sur_ben,
       sum(rsv2_re)        as sum_rsv2_re
from insurance_app.policy_result
where month = ${calc_month}
group by direction;

--5.2 高净值客户比例
insert overwrite table insurance_app.app_agg_month_high_net_rate partition (month=${calc_month})
select sum(if(income>=10000000,1,0)) /count(1) as high_net_rate
from insurance_ods.policy_client;


--新业务价值率（NBEV，New Business Embed Value）= PV（预期各年利润） / 首年保费收入
insert overwrite table app_agg_month_nbev partition (month=${calc_month})
select a.insur_code,a.insur_name,
       sum(r.prem*r.nbev)/sum(r.prem)
from insurance_ods_policy_benefit a
join insurance_ods.policy_client c on a.user_id=c.user_id
join insurance_ods.prem_std_real r
on r.age_buy=a.age_buy
and r.sex=c.sex
and r.ppp=a.ppp
group by a.insur_code,a.insur_name;

