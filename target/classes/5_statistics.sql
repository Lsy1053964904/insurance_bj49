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