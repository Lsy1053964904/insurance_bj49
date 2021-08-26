use insurance_dw;
--步骤24 计算前置的几个字段
create or replace temporary view rsv_src1 as
select a.age_buy,
       nursing_age,
       a.sex,
       t_age,
       a.ppp,
       a.bpp,
       interest_rate,
       a.sa,
       policy_year,
       age,
       qx,
       kx,                      --	残疾死亡占死亡的比例
       qx_d,                    --	扣除残疾的死亡率
       qx_ci,                   --	残疾率
       dx_d,
       dx_ci,
       lx,                      --	有效保单数
       lx_d,                    --	健康人数
       cx,
       cx_,
       ci_cx,
       ci_cx_,
       dx,
       dx_d_,
       ppp_,                    --	是否在缴费期间
       bpp_,                    --	是否在保险期间
       if(policy_year = 1,
          0.5 * (a.sa * a.db1 * pow(1 + a.interest_rate, -0.25) + b.prem * pow(1 + interest_rate, 0.25)),
          a.sa * a.db1) as db1, --	残疾给付
       db2_factor,              --	长期护理保险金给付因子
       if(policy_year = 1,
          0.5 * (a.sa * a.db2 * pow(1 + a.interest_rate, -0.25) + b.prem * pow(1 + interest_rate, 0.25)),
          a.sa * a.db2) as db2,
       sa * db3         as db3, --	养老关爱金
       b.prem * db4     as db4, --	身故给付保险
       b.prem * db5     as db5,
       b.prem
from insurance_dw.prem_src a
join insurance_ods.prem_std_real b
    on a.age_buy = b.age_buy and a.sex = b.sex and a.ppp = b.ppp;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from rsv_src1
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;

--步骤25 计算PVDB1、PVDB2、PVDB3、PVDB4、PVDB5字段
create or replace temporary view rsv_src2 as
select *,
       sum(ci_cx_*db1) over (partition by age_buy,sex,ppp order by policy_year desc)/dx as pvdb1,
       sum(ci_cx_*db2) over (partition by age_buy,sex,ppp order by policy_year desc)/dx as pvdb2,
       sum(dx*db3) over (partition by age_buy,sex,ppp order by policy_year desc)/dx as pvdb3,
       sum(cx_*db4) over (partition by age_buy,sex,ppp order by policy_year desc)/dx as pvdb4,
       sum(ci_cx_*db5) over (partition by age_buy,sex,ppp order by policy_year desc)/dx as pvdb5
       from rsv_src1;
--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from rsv_src2
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;


--步骤26 计算prem_rsv值
--步骤27 计算alpha值
--步骤28 计算beta值
--方式一：将3个汇总的指标放在一个临时中间表中
--步骤26 计算prem_rsv值
create or replace temporary view prem_rsv_temp1 as
select age_buy,sex,ppp,
       sum(if(policy_year=1,pvdb1+pvdb2+pvdb3+pvdb4+pvdb5,0))
           /sum(dx*ppp_)*sum(if(policy_year=1,dx,0)) as prem_rsv
from rsv_src2
group by age_buy,sex,ppp;

--步骤27 计算alpha值

create or replace temporary view prem_rsv_temp2 as
select a.age_buy,a.sex,a.ppp,prem_rsv,
       if(a.ppp=1,b.prem_rsv,
           sum(if(policy_year=1,
                  ((db1+db2+db5)* ci_cx_+ db3 * dx+ cx_ * db4)/ dx
               ,0))
           ) alpha --修正纯保费首年
 from rsv_src2 a
join prem_rsv_temp1 b
on a.age_buy=b.age_buy
and a.sex=b.sex
and a.ppp=b.ppp
group by a.age_buy,a.sex,a.ppp,prem_rsv;

--步骤28 计算beta值
create or replace temporary view prem_rsv_temp3 as
select age_buy,sex,ppp,prem_rsv,alpha,
       if(ppp=1,0,
          prem_rsv+(prem_rsv-alpha)/x *y
           ) as beta --修正纯保费续年
from
(select a.age_buy,a.sex,a.ppp,prem_rsv,alpha,
       sum(if(policy_year>=2,dx*ppp_,0)) as x,
       sum(if(policy_year=1,dx,0)) as y
from rsv_src2 a
join prem_rsv_temp2 b
on a.age_buy=b.age_buy
and a.sex=b.sex
and a.ppp=b.ppp
group by a.age_buy,a.sex,a.ppp,prem_rsv,alpha)  t;

--查看聚合后的数据
select *
from prem_rsv_temp3
where sex = 'M'
  and ppp = 10
  and age_buy = 18;


--方式二：为上面的明细主表cv_src2，添加3列，prem_rsv列、alpha列、beta列.
--create or replace temporary view prem_rsv_temp_b1 as
create or replace temporary view rsv_src3 as
select *,
       sum(if(policy_year=1,pvdb1+pvdb2+pvdb3+pvdb4+pvdb5,0)) over(partition by age_buy,ppp,sex )/
       sum(dx*ppp_) over(partition by age_buy,ppp,sex )*
       sum(if(policy_year=1,dx,0)) over(partition by age_buy,ppp,sex )
           as prem_rsv -- 保险费(Preuim)
from rsv_src2;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from rsv_src3
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;

--步骤27 计算alpha值
--create or replace temporary view prem_rsv_temp_b2 as
create or replace temporary view rsv_src4 as
select *,
       if(ppp=1,prem_rsv,
          ((sum(if(policy_year=1,db1,0)) over (partition by age_buy,ppp,sex)+
            sum(if(policy_year=1,db2,0))over (partition by age_buy,ppp,sex)+
            sum(if(policy_year=1,db5,0))over (partition by age_buy,ppp,sex))*
           sum(if(policy_year=1,ci_cx_,0))over (partition by age_buy,ppp,sex)+
           sum(if(policy_year=1,db3,0))over (partition by age_buy,ppp,sex) *
           sum(if(policy_year=1,dx,0))over (partition by age_buy,ppp,sex)+
           sum(if(policy_year=1,cx_,0))over (partition by age_buy,ppp,sex) *
           sum(if(policy_year=1,db4,0))over (partition by age_buy,ppp,sex))/
          sum(if(policy_year=1,dx,0))over (partition by age_buy,ppp,sex)
           ) alpha --修正纯保费首年
from rsv_src3;
--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from rsv_src4
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;

--步骤28 计算beta值
--create or replace temporary view prem_rsv_temp_b3 as
create or replace temporary view rsv_src5 as
select *,
       if(ppp=1,0,
          prem_rsv+(prem_rsv-alpha)/sum(if(policy_year>=2,dx*ppp_,0))over (partition by age_buy,ppp,sex)*
                    sum(if(policy_year=1,dx,0)) over (partition by age_buy,ppp,sex)
           ) as beta
from rsv_src4;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from rsv_src5
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;

--步骤29 修正纯保费 np_
create or replace temporary view rsv_src6 as
select *,
       if(policy_year=1,alpha,least(prem,beta))*ppp_ as np_ --修正纯保费
from rsv_src5;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from rsv_src6
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;

--步骤30 修正纯保费现值 PVNP
create or replace temporary view rsv_src7 as
select *,
       sum(dx*np_) over(partition by age_buy,sex,ppp order by policy_year desc) /dx as PVNP --修正纯保费现值
from rsv_src6;

--步骤31 准备金年末 rsv1
create or replace temporary view rsv_src8 as
select *,
       lead(pvdb1+pvdb2+pvdb3+pvdb4+pvdb5-pvnp) over(partition by age_buy,sex,ppp order by policy_year) rsv1 --准备金年末
       from rsv_src7;
--步骤32 准备金年初（未加当年初纯保费）rsv2
create or replace temporary view rsv_src9 as
select * ,
       lag(rsv1) over(partition by age_buy,sex,ppp order by policy_year) as rsv2
from rsv_src8;

--步骤33 计算修正责任准备金年末和年初
--修正责任准备金年末 rsv1_re
--修正责任准备金年初(未加当年初纯保费）rsv2_re
set spark.sql.shuffle.partitions = 8;
drop table if exists rsv_src10;
cache table rsv_src10 as
select a.*,
       greatest(rsv1, b.cv_1a)                                                                       as rsv1_re,--修正责任准备金年末
       greatest(rsv2, lag(b.cv_1b) over (partition by a.age_buy,a.sex,a.ppp order by a.policy_year)) as rsv2_re--修正责任准备金年初(未加当年初纯保费）
from rsv_src9            a
join insurance_dw.cv_src b
     on a.age_buy = b.age_buy and a.sex = b.sex and a.ppp = b.ppp and a.policy_year = b.policy_year
;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from rsv_src10
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;

--保存数据到DW结果表rsv_src
insert overwrite table insurance_dw.rsv_src
select age_buy,
       nursing_age,
       sex,
       t_age,
       ppp,
       bpp,
       interest_rate,
       sa,
       policy_year,
       age,
       qx,
       kx,
       qx_d,
       qx_ci,
       dx_d,
       dx_ci,
       lx,
       lx_d,
       cx,
       cx_,
       ci_cx,
       ci_cx_,
       dx,
       dx_d_,
       ppp_,
       bpp_,
       db1,
       db2_factor,
       db2,
       db3,
       db4,
       db5,
       np_,
       pvnp,
       pvdb1,
       pvdb2,
       pvdb3,
       pvdb4,
       pvdb5,
       prem_rsv,
       alpha,
       beta,
       rsv1,
       rsv2,
       rsv1_re,
       rsv2_re
from rsv_src10;
