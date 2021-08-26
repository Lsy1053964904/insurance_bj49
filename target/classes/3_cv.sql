--步骤13 基于prem_src表获取前置的几个字段
--由于在计算第20步骤的rt字段时，policy_year=0的情况是有意义的，所以此时
--预先拼接（union all）上policy_year=0的数据
create or replace temporary view cv_src1 as
select age_buy,
       p.nursing_age,
       sex,
       p.t_age,
       ppp,
       bpp,
       p.interest_rate,
       i.interest_rate_cv,
       p.sa,
       policy_year,
       age,
       qx,
       kx,                                                --	残疾死亡占死亡的比例
       qx_d,                                              --	扣除残疾的死亡率
       qx_ci,                                             --	残疾率
       dx_d,
       dx_ci,
       lx,                                                --	有效保单数
       lx_d,                                              --	健康人数
       dx_d / pow(1 + i.interest_rate_cv, age + 1) as cx, --
       ppp_,                                              --	是否在缴费期间
       bpp_,                                              --	是否在保险期间
       expense,                                           --	附加费用率
       db1,                                               --	残疾给付
       db2_factor,                                        --	长期护理保险金给付因子
       db3,                                               --	养老关爱金
       db4                                                --	身故给付保险
from insurance_dw.prem_src p
join input                 i on 1 = 1
union all
select distinct p.age_buy,
                p.nursing_age,
                p.sex,
                p.t_age,
                p.ppp,
                p.bpp,
                p.interest_rate,
                i.interest_rate_cv,
                p.sa,
                0    as policy_year,
                null as age,
                null as qx,
                null as kx,         --	残疾死亡占死亡的比例
                null as qx_d,       --	扣除残疾的死亡率
                null as qx_ci,      --	残疾率
                null as dx_d,
                null as dx_ci,
                null as lx,         --	有效保单数
                null as lx_d,       --	健康人数
                null as cx,
                null as ppp_,       --	是否在缴费期间
                null as bpp_,       --	是否在保险期间
                null as expense,    --	附加费用率
                null as db1,        --	残疾给付
                null as db2_factor, --	长期护理保险金给付因子
                null as db3,        --	养老关爱金
                null as db4         --	身故给付保险
from insurance_dw.prem_src p
join input                 i on 1 = 1
;
set spark.sql.decimalOperations.allowPrecisionLoss=false;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from cv_src1
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;


--步骤14 计算调整的死亡发生概率cx和当期发生重疾的概率ci_cx
create or replace temporary view cv_src2 as
select *,
       cx*pow(1+interest_rate_cv,0.5) as cx_,--调整的死亡发生概率
       dx_ci/pow(1+interest_rate_cv,age+1) as ci_cx--当期发生重疾的概率
from cv_src1;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from cv_src2
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;

--步骤15 计算ci_cx_ 、dx 、dx_d
create or replace temporary view cv_src3 as
select *,
       ci_cx*pow(1+interest_rate_cv,0.5) as ci_cx_, --当期发生重疾的概率，调整
       lx/pow(1+interest_rate_cv,age) as dx,--有效保单生存因子
       lx_d/pow(1+interest_rate_cv,age) as dx_d_--健康人数生存因子
from cv_src2;
--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from cv_src3
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;

--步骤16 计算db2、db5字段

create or replace temporary view cv_src4 as
select *,
       sum(dx * db2_factor) over (partition by sex,ppp,age_buy order by policy_year desc) / dx as DB2, --长期护理保险金
       (sum(dx * ppp_)
            over (partition by sex,ppp,age_buy order by policy_year rows between 1 following and unbounded following) /
        dx) * pow(1 + interest_rate_cv, 0.5)                                                   as DB5  --豁免保费因子
from cv_src3;
--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from cv_src4
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;

--步骤17 计算保单价值准备金毛保险费的9个参数
create or replace temporary view cv_src4_a as
select *,
       if(policy_year = 1,
           0.5 * ci_cx_ * db1 * pow(1 + interest_rate_cv, -0.25),
           ci_cx_ * db1) as A
from cv_src4;

create or replace temporary view cv_src4_b as
select age_buy, sex, ppp, sum(A) as T11
from cv_src4_a
group by age_buy, sex, ppp;

--将上面的2句合为一句：
create or replace temporary view prem_cv_temp17 as
select age_buy,
       sex,
       ppp,
       sa,
       sum(if(policy_year = 1, 0.5 * ci_cx_ * db1 * pow(1 + interest_rate_cv, -0.25), ci_cx_ * db1)) as T11,
       sum(if(policy_year = 1, 0.5 * ci_cx_ * db2 * pow(1 + interest_rate_cv, -0.25), ci_cx_ * db2)) as V11,
       sum(dx * db3)                                                                                 as W11,
       0.5 * sum(if(policy_year = 1, ci_cx_, 0)) * pow(1 + interest_rate_cv, 0.25)                   AS T9,
       0.5 * sum(if(policy_year = 1, ci_cx_, 0)) * pow(1 + interest_rate_cv, 0.25)                   AS V9,
       sum(cx_ * db4)                                                                                as X11,
       sum(ci_cx_ * DB5)                                                                             as Y11,
       sum(dx * ppp_)                                                                                as Q11,
       sum(dx * expense)                                                                             as S11
from cv_src4
group by age_buy, sex, ppp,sa,interest_rate_cv;


--步骤18 计算保单价值准备金毛保险费prem_cv
create or replace temporary view prem_cv_temp18 as
 select p.*,
        (SA*(T11+V11+W11)+s.PREM*(T9+V9+X11+Y11))/(Q11-S11) prem_cv
from    prem_cv_temp17 p
join insurance_dw.prem_std  s
on p.age_buy=s.age_buy
and p.sex=s.sex
and p.ppp=s.ppp;

select *
from prem_cv_temp18
where sex = 'M'
  and ppp = 10
  and age_buy = 18;

--对prem_cv_temp18的结果进行验证
select p.age_buy,
       p.sex,
       p.ppp,
       p.prem_cv as                        my_prem_cv,
       r.prem_cv as                        real_prem_cv,
       (p.prem_cv - r.prem_cv) / r.prem_cv diff_rate_prem_cv
from prem_cv_temp18             p
join insurance_ods.prem_cv_real r on p.age_buy = r.age_buy and p.sex = r.sex and p.ppp = r.ppp
where (p.prem_cv - r.prem_cv) / r.prem_cv > 0.001
;

--将聚合的中间结果保存到prem_cv 表
insert overwrite table insurance_dw.prem_cv
select age_buy,sex,ppp ,prem_cv from prem_cv_temp18;

--步骤19 计算净保费np_、pvnp、pvdb1~5字段
create or replace temporary view cv_src5 as
select c.*,
       (ppp_-expense)*p.prem_cv as np_,--净保费
       prem_cv*sum(dx*(ppp_-expense)) over(partition by c.age_buy,c.sex,c.ppp order by policy_year desc)/dx as pvnp, --净保费现值
       if(policy_year=1,
           (sa*sum(ci_cx_*db1) over(partition by c.age_buy,c.sex,c.ppp order by policy_year rows between 1 following and unbounded following)
                +0.5*
                 (s.prem*ci_cx_*pow(1+interest_rate_cv,0.25)+sa*db1*ci_cx_*pow(1+interest_rate_cv,-0.25)))/dx
           ,
           sa*sum(ci_cx_*db1) over (partition by c.age_buy,c.sex,c.ppp order by policy_year desc)/dx
           ) as pvdb1,
       if(policy_year=1,
          (sa*sum(ci_cx_*db2) over(partition by c.age_buy,c.sex,c.ppp order by policy_year rows between 1 following and unbounded following)
              +0.5*
               (s.prem*ci_cx_*pow(1+interest_rate_cv,0.25)+sa*db2*ci_cx_*pow(1+interest_rate_cv,-0.25)))/dx
           ,
          sa*sum(ci_cx_*db2) over (partition by c.age_buy,c.sex,c.ppp order by policy_year desc)/dx
           ) as pvdb2,
       sa*sum(dx*db3)over (partition by c.age_buy,c.sex,c.ppp order by policy_year desc)/dx as pvdb3,
       s.prem*sum(cx_*db4)over(partition by c.age_buy,c.sex,c.ppp order by policy_year desc)/dx as pvdb4,
       s.prem*sum(ci_cx_*db5)over(partition by c.age_buy,c.sex,c.ppp order by policy_year desc)/dx as pvdb5
from cv_src4 c
join insurance_dw.prem_cv p
on c.age_buy=p.age_buy
and c.sex=p.sex
and c.ppp=p.ppp
join insurance_dw.prem_std s
on c.age_buy=s.age_buy
and c.sex=s.sex
and c.ppp=s.ppp
;
--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from cv_src5
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;
--步骤20 保单价值准备金pvr、rt字段
create or replace temporary view cv_src6 as
select *,
       if(policy_year=0,null,
           lead(pvdb1+pvdb2+pvdb3+pvdb4+pvdb5-pvnp)over (partition by age_buy,sex,ppp order by policy_year)
           ) as pvr,--保单价值准备金
       if(ppp=1,1,
           if(policy_year>=least(20,ppp),1,0.8+policy_year*0.8/least(20,ppp))
           ) as rt
from cv_src5;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from cv_src6
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;


--步骤21 计算修匀净保费np、生存金sur_ben、cv_1b字段
create or replace temporary view cv_src7 as
select *,
       np_ * lag(rt) over (partition by age_buy,sex,ppp order by policy_year) as                       NP,--修匀净保费
       db3 * sa                                                               as                       sur_ben, --生存金
       rt * greatest(pvr - lead(db3 * sa) over (partition by age_buy,sex,ppp order by policy_year), 0) cv_1b--现金价值年末（生存给付后）
from cv_src6;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from cv_src7
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;

--步骤22 计算现金价值年末（生存给付前）cv_1a
create or replace temporary view cv_src8 as
select *,
       cv_1b+lead(sur_ben) over(partition by age_buy,sex,ppp order by policy_year) as cv_1a --现金价值年末（生存给付前）
from cv_src7;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from cv_src8
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;

--步骤23 计算现金价值年中 cv_2
create or replace temporary view cv_src9 as
select *,
       (np+lag(cv_1b) over (partition by age_buy,sex,ppp order by policy_year)+cv_1a)/2 as cv_2
from cv_src8;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from cv_src9
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year;


--将每个保单年度的现金价值保存到结果表 insurance_dw.cv_src
insert overwrite table insurance_dw.cv_src
select age_buy,--	年投保龄
       nursing_age,--	长期护理保险金给付期满年龄
       sex,--	性别
       t_age,--	满期年龄(Terminate Age)
       ppp,--	交费期间(Premuim Payment Period PPP)
       bpp,--	保险期间(BPP)
       interest_rate_cv,--	现金价值预定利息率（Interest Rate CV）
       sa,--	基本保险金额(Baisc Sum Assured)
       policy_year,--	保单年度
       age,--	保单年度对应的年龄
       qx,--	死亡率
       kx,--	残疾死亡占死亡的比例
       qx_d,--	扣除残疾的死亡率
       qx_ci,--	残疾率
       dx_d,--
       dx_ci,--
       lx,--	有效保单数
       lx_d,--	健康人数
       cx,--	当期发生该事件的概率，如下指的是死亡发生概率
       cx_,--	对Cx做调整，不精确的话，可以不做
       ci_cx,--	当期发生重疾的概率
       ci_cx_,--	当期发生重疾的概率，调整
       dx,--	有效保单生存因子
       dx_d_,--	健康人数生存因子
       ppp_,--	是否在缴费期间，1-是，0-否
       bpp_,--	是否在保险期间，1-是，0-否
       expense,--	附加费用率
       db1,--	残疾给付
       db2_factor,--	长期护理保险金给付因子
       db2,--	长期护理保险金
       db3,--	养老关爱金
       db4,--	身故给付保险金
       db5,--	豁免保费因子
       np_,--	净保费
       pvnp,--	净保费现值
       pvdb1,--
       pvdb2,--
       pvdb3,--
       pvdb4,--
       pvdb5,--
       pvr,--	保单价值准备金
       rt,--
       np,--	修匀净保费
       sur_ben,--	生存金
       cv_1a,--	现金价值年末（生存给付前）
       cv_1b,--	现金价值年末（生存给付后）
       cv_2 --	现金价值年中
from cv_src9;

