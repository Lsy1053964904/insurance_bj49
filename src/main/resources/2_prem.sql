use insurance_ods;
set spark.sql.shuffle.partitions=4;
--创建一个起始表，包含
--投保年龄（Age)	长期护理保险金给付期满年龄	性别(Sex)	满期年龄(Terminate Age)	缴费期间(Premuim Payment Period PPP)	保险期间(BPP)	预定利息率(Interest Rate PREM&RSV)	基本保险金额(Baisc Sum Assured)	保单年度（policy year)	年龄（Age)

--生成一个性别表
cache table sex_table as
select stack(2,'M','F') sex;
--查看表数据
select * from sex_table;

--生成一个缴费期表
cache table jfq_table as
select stack(4,10,15,20,30) ppp;
--查看表数据
select * from jfq_table;

--生成一个序列 方式1
cache table sequance_100_1 as
select row_number() over(order by 1) id from (select explode( array_repeat(0,100)) id) t ;

--生成一个序列 方式2
cache table sequance_100 as
select explode( sequence(1,100)) id;


--将多个固定参数保存到一张表中，表只有一行数据。后期方便使用。
cache table input as
select 0.035  interest_rate,    --预定利息率(Interest Rate PREM&RSV)
       0.055  interest_rate_cv,--现金价值预定利息率（Interest Rate CV）
       0.0004 acci_qx,--意外身故死亡发生率(Accident_qx)
       0.115  rdr,--风险贴现率（Risk Discount Rate)
       10000  sa,--基本保险金额(Baisc Sum Assured)
       1      average_size,--平均规模(Average Size)
       1      MortRatio_Prem_0,--Mort Ratio(PREM)
       1      MortRatio_RSV_0,--Mort Ratio(RSV)
       1      MortRatio_CV_0,--Mort Ratio(CV)
       1      CI_RATIO,--CI Ratio
       6      B_time1_B,--生存金给付时间(1)—begain
       59     B_time1_T,--生存金给付时间(1)-terminate
       0.1    B_ratio_1,--生存金给付比例(1)
       60     B_time2_B,--生存金给付时间(2)-begain
       106    B_time2_T,--生存金给付时间(2)-terminate
       0.1    B_ratio_2,--生存金给付比例(2)
       70     MB_TIME,--祝寿金给付时间
       0.2    MB_Ration,--祝寿金给付比例
       0.7    RB_Per,--可分配盈余分配给客户的比例
       0.7    TB_Per,--未分配盈余分配给客户的比例
       1      Disability_Ratio,--残疾给付保险金保额倍数
       0.1    Nursing_Ratio,--长期护理保险金保额倍数
       75     Nursing_Age ;--长期护理保险金给付期满年龄
--查看表数据
select * from input;

--Spark 2.x版本中默认不支持笛卡尔积操作，需要手动开启
set spark.sql.crossJoin.enabled=true;


--生成一个数据不重复的表，包含所有不同性别，不同缴费期，不同投保年龄，在未来每个不同的保单年度的组合。方便后续在此基础上计算每个组合的应交保险费，以及的生存金，现金价值，准备金。
create or replace temporary view prem_src0 as
select s.sex,--性别
       j.ppp,--缴费期
       seq.id               as age_buy, --投保年龄
       i.nursing_age,--长期护理保险金给付期满年龄
       i.interest_rate,--预定利息率(Interest Rate PREM&RSV)
       i.sa,--基本保险金额(Baisc Sum Assured)
       106                  as t_age, --满期年龄
       106 - seq.id         as bpp, --保障期间
       seq2.id              as policy_year, --保单年度,
       seq.id + seq2.id - 1 as age --在某个policy_year的对应的年龄
from sex_table    s
join jfq_table    j on 1 = 1
--这个序列表的职责是生成投保年龄
join sequance_100 seq on seq.id >= 18 and seq.id <= 70 - j.ppp
--这个序列表的职责是生成未来的保单年度
join sequance_100 seq2 on seq2.id >= 1 and seq2.id <= 106 - seq.id
--input表只有一行数据，笛卡尔积也不会影响性能问题。
join input        i on 1 = 1;

--查看数据，用作跟Excel《精算数据表》的案例对比。
select * from prem_src0 where sex='M' and ppp=15 and age_buy=18;




--步骤1 计算是否在缴费期内ppp_、bpp_字段
create or replace temporary view prem_src1 as
select * ,
       --case when policy_year<=ppp then 1
       --    else 0
       --end    as ppp_,  --是否在缴费期内
       if(policy_year<=ppp,1,0) ppp_,
       if(policy_year<=bpp,1,0) as bpp_  --是否在保险期间内
from prem_src0;
--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select * from prem_src1 where sex='M' and ppp=10 and age_buy=18 order by policy_year limit 5;

--步骤2 计算死亡率qx、kx、qx_ci字段
create or replace temporary view prem_src2 as
select p.*,
       (case when p.age<=105 then
                 if(p.sex='M',m.cl1,m.cl2)
             else 0
           end)*i.mortratio_prem_0*bpp_ as qx, --死亡率
       (case when p.age<=105 then
                 if(p.sex='M',d.k_male,d.k_female)
             else 0
           end)*bpp_ as kx, --残疾死亡占死亡的比例
       if(p.sex='M',d.male,d.female)*bpp_ as qx_ci  --残疾率
from prem_src1 p
join insurance_ods.mort_10_13 m on p.age=m.age
join insurance_ods.dd_table d on p.age=d.age
join input i on 1=1
;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from prem_src2
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year limit 5;

--步骤3 计算qx_d字段
--需要禁止精度损失
set spark.sql.decimalOperations.allowPrecisionLoss=false;
create or replace temporary view prem_src3 as
select *,
       if(age=105,qx-qx_ci,qx*(1-kx))*bpp_ qx_d  --扣除残疾的死亡率
from prem_src2;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from prem_src3
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year  ;



--步骤4 计算有效保单数lx字段
--分步骤1、只考虑第一保单年度的情况
create or replace temporary view prem_src4 as
select *,
       if(policy_year=1,1d,null) as lx --有效保单数
from prem_src3;
--查询若干条数据，
select *
from prem_src4
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year  ;

--需要提前将UDAF类所在的jar包上传到node3上。
--/export/server/spark/sbin/start-thriftserver.sh \
--  --jars /opt/insurance_jb49-1.0.jar \
--  --hiveconf hive.server2.thrift.port=10001 \
--  --hiveconf hive.server2.thrift.bind.host=node3 \
--  --master local[*]
--注册一个UDAF函数
add jar '/opt/insurance_jb49-1.0.jar';

create or replace temporary function UDAFLx as 'cn.itcast.util.UDAFLx'
    using jar '/opt/insurance_jb49-1.0.jar' ;

--用UDAF函数来计算后续所有的lx字段
create or replace temporary view prem_src4_2 as
select sex,
       ppp,
       age_buy,
       Nursing_Age,
       interest_rate,
       sa,
       t_age,
       bpp,
       policy_year,
       age,
       ppp_,
       bpp_,
       qx,
       kx,
       qx_ci,
       qx_d,
       --用UDAF函数来计算后续所有的lx字段
       UDAFLx(lx,qx) over (partition by sex,ppp,age_buy order by policy_year) as lx
from prem_src4;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from prem_src4_2
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year  ;

--步骤5 计算dx_d、dx_ci、lx_d字段
--分步骤1 ，只计算policy_year=1时的3个指标

create or replace temporary view prem_src5 as
select *,
       if(policy_year=1,1d,null) as lx_d, --健康人数
       if(policy_year=1,qx_d,null) as dx_d,
       if(policy_year=1,qx_ci,null) as dx_ci
from prem_src4_2;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from prem_src5
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year  ;

--注册UDAF函数
create or replace temporary function UDAFDxdDxciLxd as 'cn.itcast.util.UDAFDxdDxciLxd'
 using jar '/opt/insurance_jb49-1.0.jar' ;
--用UDAF函数直接计算所有的dx_d、dx_ci、lx_d字段
create or replace temporary view prem_src5_1 as
select sex, ppp, age_buy, Nursing_Age, interest_rate, sa,
       t_age,
       bpp, policy_year, age, ppp_,
       bpp_, qx, kx, qx_ci,
       qx_d, lx,
       obj.lx_d, --健康人数
       obj.dx_d,
       obj.dx_ci
from (select sex,
             ppp,
             age_buy,
             Nursing_Age,
             interest_rate,
             sa,
             t_age,
             bpp,
             policy_year,
             age,
             ppp_,
             bpp_,
             qx,
             kx,
             qx_ci,
             qx_d,
             lx,
             --无法分别计算3个指标。
             --UDAFDxdDxciLxd(参数1，参数2，。。) over(partition by sex,ppp,age_buy order by policy_year) lx_d, --健康人数
             --UDAFDxdDxciLxd(参数1，参数2，。。) over(partition by sex,ppp,age_buy order by policy_year) dx_d,
             --UDAFDxdDxciLxd(参数1，参数2，。。) over(partition by sex,ppp,age_buy order by policy_year) dx_ci
             --只能一次性计算完3个指标（一个结果包含了3个指标），最后再将3个指标拆分出来。
             UDAFDxdDxciLxd(lx_d,qx_d,qx_ci) over (partition by sex,ppp,age_buy order by policy_year) as obj
      from prem_src5);
--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from prem_src5_1
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year  ;


--步骤6 计算cx字段
--cache table prem_src6 as
create or replace temporary view prem_src6 as
select * ,
       dx_d/pow(1+interest_rate,age+1) as cx --当期发生该事件的概率，如下指的是死亡发生概率
from prem_src5_1;
--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from prem_src6
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year  ;
--步骤7 计算 cx、ci_cx字段

create or replace temporary view prem_src7 as
select *,
       cx*pow(1+interest_rate,0.5) as cx_, --对cx做调整，不精确的话，可以不做 cx_
       dx_ci/pow(1+interest_rate,age+1) as ci_cx --当期发生重疾的概率
from prem_src6;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from prem_src7
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year  ;
--步骤8 计算ci_cx_、dx、dx_d字段
create or replace temporary view prem_src8 as
select *,
       ci_cx*pow(1+interest_rate,0.5) as ci_cx_,--当期发生重疾的概率，调整
       lx/pow(1+interest_rate,age) as dx,--有效保单生存因子
       lx_d/pow(1+interest_rate,age) as dx_d_--健康人数生存因子
from prem_src7;
--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from prem_src8
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year  ;


--步骤9 计算附加费用率expense、db1、db2_factor字段
create or replace temporary view prem_src9 as
select p.*,
       i.nursing_ratio,
           --方式1
/*          (case when policy_year=1 then r.r1
               when policy_year=2 then r.r2
               when policy_year=3 then r.r3
               when policy_year=4 then r.r4
               when policy_year=5 then r.r5
               when policy_year>=6 then r.r6_
          end)*ppp_    as expense, --附加费用率*/
       --方式2
       element_at(array(r1, r2, r3, r4, r5, r6_), if(policy_year >= 6, 6, policy_year))*ppp_ as expense,   --附加费用率
       ----方式3
    /* elt(if(policy_year>=6,6,policy_year),r1,r2,r3,r4,r5,r6_)*ppp_  as expense2--附加费用率*/
       i.disability_ratio * bpp_                                                        as db1,       --残疾给付 DB1
       if(age < p.Nursing_Age, 1, 0) * i.nursing_ratio                                  as db2_factor --长期护理保险金给付因子 db2_factor
from prem_src8                       p
join insurance_ods.pre_add_exp_ratio r on p.ppp = r.ppp
join input                           i on 1 = 1;
--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from prem_src9
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year  ;

--步骤10 计算db2、db3、db4、db5字段

create or replace temporary view prem_src10 as
select *,
       sum(dx * db2_factor) over (partition by sex,ppp,age_buy order by policy_year desc )/dx as DB2, --步骤10长期护理保险金
       if(age >= Nursing_Age, 1, 0) * nursing_ratio                                        as DB3, --养老关爱金
       least(ppp, policy_year)                                                             as DB4, --身故给付保险金
       (sum(dx * ppp_)
            over (partition by sex,ppp,age_buy order by policy_year rows between 1 following and unbounded following) /
        dx) * pow(1 + interest_rate, 0.5)                                                  as DB5  --豁免保费因子
from prem_src9;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from prem_src10
where sex = 'M'
  and ppp = 10
  and age_buy = 18
order by policy_year  ;


--保存结果表 DW层prem_src 表
insert overwrite table insurance_dw.prem_src
select
    age_buy,
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
    expense,
    db1,
    db2_factor,
    db2,
    db3,
    db4,
    db5
from prem_src10 ;

--步骤11 聚合计算中间参数
create or replace temporary view prem_std11 as
select age_buy,
       sex,
       ppp,
       interest_rate,
       sa,
       bpp,
       sum(if(policy_year = 1, 0.5 * ci_cx_ * db1 * pow(1 + interest_rate, -0.25), ci_cx_ * db1)) as T11,
       sum(if(policy_year = 1, 0.5 * ci_cx_ * db2 * pow(1 + interest_rate, -0.25), ci_cx_ * db2)) as V11,
       sum(dx * db3)                                                                              as W11,
       sum(dx * ppp_)                                                                             as Q11,
       0.5 * sum(if(policy_year = 1, ci_cx_, 0)) * pow(1 + interest_rate, 0.25)                   as T9,
       0.5 * sum(if(policy_year = 1, ci_cx_, 0)) * pow(1 + interest_rate, 0.25)                   as V9,
       sum(dx * expense)                                                                          as S11,
       sum(cx_ * db4)                                                                             as X11,
       sum(ci_cx_ * db5)                                                                          as Y11
from insurance_dw.prem_src
group by age_buy, sex, ppp,interest_rate,sa,bpp
;


--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from prem_std11
where sex = 'M'
  and ppp = 10
  and age_buy = 18 ;
--步骤12 计算期交保费
create or replace temporary view prem_std12 as
select age_buy,
       sex,
       ppp,
       bpp,
       sa*(T11+V11+W11)/(Q11-T9-V9-S11-X11-Y11) as prem
       from prem_std11 ;

--查询若干条数据，用作跟Excel《精算数据表》的案例对比。
select *
from prem_std12
where sex = 'M'
  and ppp = 10
  and age_buy = 18 ;

--检查所有的保费数据是否准确。
select p.age_buy,
       p.sex,
       p.ppp,
       round(p.prem)              as my_prem,
       r.prem                     as real_prem,
       (p.prem - r.prem) / r.prem as diff_rate_prem
from prem_std12                  p
join insurance_ods.prem_std_real r on p.age_buy = r.age_buy and p.sex = r.sex and p.ppp = r.ppp
where abs((p.prem - r.prem) / r.prem) > 0.001
order by age_buy, sex, ppp;


--验证应交保费准确无误后，插入hive的DW层的prem_std表
insert overwrite table insurance_dw.prem_std
select age_buy,sex,ppp,bpp,prem from prem_std12;