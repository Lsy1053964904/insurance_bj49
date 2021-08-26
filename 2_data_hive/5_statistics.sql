--计算《精算数据表》

use insurance_app;;
set spark.sql.crossJoin.enabled=true;
--计算精算数据表
drop table if exists cache_policy_actuary;
cache table cache_policy_actuary as
select cv.age_buy,
       cv.sex,
       cv.ppp,
       cv.bpp,
       cv.policy_year,
       cv.sa,
       cv.cv_1a,--现金价值给付前cv_1a
       cv.cv_1b,--现金价值给付后cv_1b
       cv.sur_ben,--生存给付金sur_ben
       cv.np,--纯保费（NP）CV.NP
       rsv.rsv2_re,--年初责任准备金rsv2_re
       rsv.rsv1_re, --年末责任准备金rsv1_re
       rsv.np_ --纯保费(RSV) RSV.np_
from  insurance_dw.cv_src  cv
left join insurance_dw.rsv_src rsv
          on cv.sex = rsv.sex
              and cv.ppp = rsv.ppp
              and cv.age_buy = rsv.age_buy
              and cv.policy_year = rsv.policy_year;
--验证精算数据表
select * from cache_policy_actuary where age_buy=18 and sex='M' and ppp=10 order by policy_year limit 10;

insert overwrite table policy_actuary select * from cache_policy_actuary;

--计算客户的精算数据表
set month_calc= '2021-02';
drop table if exists cache_policy_result;
cache table cache_policy_result as
select pb.pol_no,
       p.user_id,
       p.name,
       p.sex,
       p.birthday,
       pb.ppp,
       pb.age_buy,
       pb.buy_datetime,
       pb.insur_name,
       pb.insur_code,
       p.province,
       p.city,
       p.direction,
       a.bpp,
       a.policy_year,
       a.sa,
       a.cv_1a,--现金价值给付前cv_1a
       a.cv_1b,--现金价值给付后cv_1b
       a.sur_ben,--生存给付金sur_ben
       a.np,--纯保费（NP）CV.NP
       a.rsv2_re,--年初责任准备金rsv2_re
       a.rsv1_re, --年末责任准备金rsv1_re
       a.np_, --纯保费(RSV) RSV.np_
       s.prem  prem_std,--期交保费
       case
           when floor(months_between(${month_calc}, pb.buy_datetime) / 12) + 1 <= pb.ppp
               and month(${month_calc}) = month(pb.buy_datetime) then s.prem
           else 0
           end prem_thismonth, --本月应交保费
       ${month_calc} month --当前统计月份，作为分区
from insurance_ods.policy_benefit            pb
join insurance_ods.policy_client        p on p.user_id = pb.user_id
left join cache_policy_actuary a
          on p.sex = a.sex
              and pb.ppp = a.ppp
              and pb.age_buy = a.age_buy
              and floor(months_between(${month_calc}, pb.buy_datetime) / 12) + 1 = a.policy_year
left join insurance_dw.prem_std             s
          on pb.age_buy = s.age_buy
              and pb.ppp = s.ppp
              and p.sex = s.sex
where substr(pb.buy_datetime, 0, 7) <= ${month_calc};


--验证客户的精算数据表
select *
from cache_policy_result
where age_buy = 18
  and ppp = 10
  and sex = 'M'
order by policy_year
limit 20;

--如果验证没问题，保存到hive结果表
--开启动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table policy_result partition (month)
    select * from cache_policy_result;

--保费收入增长率
-- 手动指定需要计算的月份，好处是:
-- 1、后期可以在SQL中重复地使用该变量。
-- 2、一旦指定了具体的值，那么下面的last_month_calc变量，也会自动变更，无需手动更新。
set month_calc= '2021-03';
--获取上个月的月份
set last_month_calc= substr(add_months(${month_calc},-1),0,7);

--初始化所有
insert overwrite table app_agg_month_incre_rate partition (month)
with t1 as (select month, sum(prem_thismonth) prem from insurance_app.policy_result group by month),
     t2 as (select prem,
                   lag(prem) over (order by month)                                                      last_prem,
                   round((prem - lag(prem) over (order by month)) / lag(prem) over (order by month), 4) prem_incre_rate,
                   month
            from t1)
select * from t2 where month = '2021-04';

insert overwrite table app_agg_month_incre_rate partition (month = ${month_calc})
select a.prem,
       b.prem                               last_prem,
       round((a.prem - b.prem) / b.prem, 4) prem_incre_rate
from (select ${month_calc} month,
             sum(prem_thismonth) prem
      from insurance_app.policy_result
      where month = ${month_calc})           a
left join (select ${last_month_calc} month,
                  prem
           from insurance_app.app_agg_month_incre_rate
           where month = ${last_month_calc}) b on 1 = 1;
--查看结果数据
select * from app_agg_month_incre_rate;

--首年保费与保费收入比
set month_calc= '2021-03';
insert overwrite table app_agg_month_first_of_total_prem partition (month=${month_calc})
select sum(prem)                   first_prem,
       sum(total_prem)             total_prem,
       sum(prem) / sum(total_prem) first_of_total_prem
from (
    select --总共交了多少个月,取最小。
           least(ceil(months_between(current_date, pb.buy_datetime) / 12),
                 cast(pb.ppp as int),
                 ceil(months_between(elapse_date, pb.buy_datetime) / 12)) * s.prem total_prem,
           s.prem
    from insurance_ods.policy_benefit     pb
    join      insurance_ods.policy_client p on pb.user_id = p.user_id
    left join insurance_dw.prem_std       s
              on pb.age_buy = s.age_buy
                  and pb.ppp = s.ppp
                  and p.sex = s.sex
    where substr(pb.buy_datetime, 0, 7) <= ${month_calc}) a;

--13个月续保率 13 month persistence ratio
--当前2021-03月， 13个月前是2020-3月
set spark.sql.crossJoin.enabled=true;
--初始化历史数据。
insert overwrite table app_agg_month_rate_eff13 partition (month )
select insur_code, insur_name, round(cnt_eff13 / cnt_all, 5) rate_eff13, month
from (select insur_code,
             insur_name,
             month,
             sum(if((month >= pb.buy_datetime and (month < pb.elapse_date or elapse_date is null))
                        and (elapse_date is null or substr(elapse_date, 0, 7) >= substr(add_months(month, 13), 0, 7)),
                    1,
                    0))                                                                                  cnt_eff13,
             sum(if(month >= pb.buy_datetime and (month < pb.elapse_date or elapse_date is null), 1, 0)) cnt_all
      from (
          select substr(day, 0, 7) month
          from (select add_months(current_date, (-1) * row_number() over (order by 1)) day
                from (select explode(array_repeat(1, 36)) id) a) t
      )                                      b
      left join insurance_ods.policy_benefit pb on 1 = 1
      join      insurance_ods.policy_client  c
                on pb.user_id = c.user_id
                    and month >= pb.buy_datetime and (month < pb.elapse_date or elapse_date is null)
      group by insur_code, insur_name, month) t
order by month desc;

--计算本月数据。
set month_calc= '2021-03';
insert overwrite table app_agg_month_rate_eff13 partition (month=${month_calc})
select insur_code, insur_name, round(cnt_eff13 / cnt_all, 5) rate_eff13,cnt_eff13,cnt_all
from (select insur_code,
             insur_name,
             sum(if(elapse_date is null or substr(elapse_date, 0, 7) >= substr(add_months(${month_calc}, 13), 0, 7),
                    1,
                    0)) cnt_eff13,
             count(1) cnt_all
      from insurance_ods.policy_benefit pb
      join insurance_ods.policy_client  c
           on pb.user_id = c.user_id
      where ${month_calc} >= pb.buy_datetime
        and (${month_calc} < pb.elapse_date or elapse_date is null)
      group by insur_code, insur_name
) t;
--查看结果
select * from app_agg_month_rate_eff13 order by month desc;


--个人营销渠道的件均保费
--初始化历史的每月的个人营销渠道的件均保费
insert overwrite table app_agg_month_premperpol partition (month)
select insur_code,
       insur_name,
       round(sum(prem) / count(1), 2) prem_per_pol,
       substr(buy_datetime, 0, 7)     month
from insurance_ods.policy_benefit     pb
join      insurance_ods.policy_client c
          on pb.user_id = c.user_id
left join insurance_dw.prem_std       s
          on pb.age_buy = s.age_buy
              and pb.ppp = s.ppp
              and c.sex = s.sex
group by insur_code, insur_name, substr(buy_datetime, 0, 7)
order by insur_code, insur_name, substr(buy_datetime, 0, 7)
;
--计算本月数据。
insert overwrite table app_agg_month_premperpol partition (month=${month_calc})
select insur_code,
       insur_name,
       round(sum(prem) / count(1), 2) prem_per_pol
from insurance_ods.policy_benefit     pb
join      insurance_ods.policy_client c
          on pb.user_id = c.user_id
left join insurance_dw.prem_std                    s
          on pb.age_buy = s.age_buy
              and pb.ppp = s.ppp
              and c.sex = s.sex
where substr(buy_datetime, 0, 7) = ${month_calc}
group by insur_code, insur_name
;
--查看结果
select * from app_agg_month_premperpol order by month desc;

--犹豫期保费退保率
--初始化历史的犹豫期保费退保率
insert overwrite table app_agg_month_cooloff_surr partition (month)
select insur_code,
       insur_name,
       sum(if(keep_days <= 20, prem, 0))                       cooloff,
       sum(prem)                                               cnt_all,
       round(sum(if(keep_days <= 20, prem, 0)) / sum(prem), 4) cooloff_rate,
       substr(pb.buy_datetime, 0, 7)                           month
from insurance_ods.policy_benefit        pb
join      insurance_ods.policy_client    c
          on pb.user_id = c.user_id
left join insurance_dw.prem_std          p on pb.ppp = p.ppp and c.sex = p.sex and pb.age_buy = p.age_buy
left join insurance_ods.policy_surrender s
          on pb.pol_no = s.pol_no
group by insur_code, insur_name, substr(pb.buy_datetime, 0, 7)
order by substr(pb.buy_datetime, 0, 7) desc;
--查看结果
select * from app_agg_month_cooloff_surr order by month desc ;


insert overwrite table app_agg_month_cooloff_surr partition (month=${month_calc})
select insur_code,
       insur_name,
       sum(if(keep_days <= 20, prem, 0))                       cooloff,
       sum(prem)                                               cnt_all,
       round(sum(if(keep_days <= 20, prem, 0)) / sum(prem), 4) cooloff_rate
from insurance_ods.policy_benefit        pb
join      insurance_ods.policy_client    c
          on pb.user_id = c.user_id
left join insurance_dw.prem_std          p on pb.ppp = p.ppp and c.sex = p.sex and pb.age_buy = p.age_buy
left join insurance_ods.policy_surrender s
          on pb.pol_no = s.pol_no
where substr(pb.buy_datetime, 0, 7) = ${month_calc}
group by insur_code, insur_name;

--查看结果
select * from app_agg_month_cooloff_surr limit 19;

--退保率 surrender rate
--退保率＝报告期退保金÷（期初长期险责任准备金＋报告期长期险原/分保费收入）
set month_calc= '2021-03';
--获取14个月前的月份
set last14_month_calc= substr(add_months(${month_calc},-14),0,7);

insert overwrite table app_agg_month_surr_rate partition (month=${month_calc})
select insur_code,
       insur_name,
       count(s.pol_no)                              cnt_surr,
       count(pb.pol_no)                             cnt_all,
       round(count(s.pol_no) / count(pb.pol_no), 4) surr_rate
from insurance_ods.policy_client         c
join      insurance_ods.policy_benefit   pb on c.user_id = pb.user_id
left join insurance_ods.policy_surrender s
          on pb.pol_no = s.pol_no
              and substr(pb.elapse_date, 0, 7) > ${last14_month_calc}
              and substr(pb.elapse_date, 0, 7) <= ${month_calc}
--14个月前是有效保单。
where substr(pb.buy_datetime, 0, 7) <= ${last14_month_calc}
  and (pb.elapse_date > ${last14_month_calc} or pb.elapse_date is null)
group by insur_code, insur_name;
--查看结果
select * from app_agg_month_surr_rate ;

--死亡发生率 =在月末时点，统计每个年龄的人群，按一岁一组，计算其中历史所有发生过死亡的保单数/所有的有效保单
--残疾发生率 =在月末时点，统计每个年龄的人群，按一岁一组，计算其中历史所有发生过残疾的保单数/所有的有效保单
set month_calc= '2021-03';
insert overwrite table app_agg_month_mort_dis_rate partition (month=${month_calc})
select insur_code,
       insur_name,
       a.age,
       a.sg_cnt,
       a.sc_cnt,
       all_cnt,
       round(a.sg_cnt / all_cnt, 8) sg_rate,
       round(a.sc_cnt / all_cnt, 8) sc_rate
from (select p.insur_code,
             p.insur_name,
             ceil(months_between(c.claim_date, c.birthday) / 12) age,
             sum(if(c.claim_item like '%sgbxj%', 1, 0))          sg_cnt,
             sum(if(c.claim_item like '%scbxj%', 1, 0))          sc_cnt,
             sum(count(1)) over ()                               all_cnt
      from insurance_ods.policy_client         c
      join      insurance_ods.policy_benefit   p on c.user_id = p.user_id
      left join insurance_ods.claim_info c
                on p.pol_no = c.pol_no
                    and substr(c.claim_date, 0, 7) <= ${month_calc}
      where substr(p.buy_datetime, 0, 7) <= ${month_calc}
        and (p.elapse_date is null or substr(p.elapse_date, 0, 7) >= ${month_calc})
      group by p.insur_code,p.insur_name, age) a
order by age desc;

--统计分析  计算全国各区域的投保人数，和保费汇总，并按照投保人数倒序排序。
insert overwrite table app_agg_month_dir partition (month=${month_calc})
select direction,
       count(user_id)                sum_users,  --总投保人数
       round(sum(prem_thismonth), 2) sum_prem,--当月保费汇总
       round(sum(cv_1b), 2)          sum_cv_1b,--总现金价值
       round(sum(sur_ben), 2)        sum_sur_ben,--总生存金
       round(sum(rsv2_re), 2)        sum_rsv2_re--总准备金
from insurance_app.policy_result
where month = ${month_calc}
group by direction;
select * from app_agg_month_dir;

--新业务价值率
set month_calc= '2021-03';
insert overwrite table app_agg_month_nbev partition (month=${month_calc})
select insur_code,
       insur_name,
       sum(r.prem * r.nbev) / sum(prem) nbev
from insurance_ods.policy_client  c
join insurance_ods.policy_benefit p on c.user_id = p.user_id
join insurance_ods.prem_std_real  r on p.age_buy = r.age_buy
    and c.sex = r.sex and p.ppp = r.ppp
where substr(buy_datetime, 0, 7) <= ${month_calc}
group by insur_code, insur_name;
--查看结果
select * from app_agg_month_nbev limit 4;

--高净值客户比例
set month_calc= '2021-03';
insert overwrite table app_agg_month_high_net_rate partition (month=${month_calc})
select sum(if(income>=10000000,1,0))/count(1) high_net_rate from insurance_ods.policy_client ;

