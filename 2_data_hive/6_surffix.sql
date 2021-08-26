--计算《精算数据表》

use insurance_ods;

cache table sex as
select stack(2, 'M', 'F') sex;

cache table jfq as
select stack(4, 10, 15, 20, 30) ppp;

cache table id200 as
select row_number() over (order by id) id
from (select explode(array_repeat(1, 200)) id) a;


set spark.sql.crossJoin.enabled=true;

--造客户保单
/*
id	name	sex	age	ppp	buy_datetime
1	张三	   M	30	10	2020/12/9
2	李四	   F	23	15	2018/10/5

*/
drop table policy;
cache table cn.itcast.policy as
select 1 id, '张三' name, 'M' sex, 30 age_buy, 10 ppp, '2020-12-09' buy_datetime
union all
select 2, '李四', 'F' sex, 23, 15, '2018-10-05';

select *
from policy;
select *
from cache_policy_info
limit 1;

--批量随机造保单
Select cast(floor(rand() * 10) + 1 as int) n;
select element_at(array('张', '李', '王', '赵', '孙', '陈', '刘', '郭', '彭', '杨'), 1) as first_name;
select element_at(array('张', '李', '王', '赵', '孙', '陈', '刘', '郭', '彭', '杨'), cast(ceil(rand()*10) as int)) as first_name;
select element_at(array('平', '强', '华', '龙', '杰', '丽', '霞', '雪', '芳', '思'), cast(ceil(rand()*10) as int)) as last_name;

select concat(
               element_at(array('张', '李', '王', '赵', '孙', '陈', '刘', '郭', '彭', '杨'), cast(ceil(rand()*10) as int)),
               element_at(array('平', '强', '华', '龙', '杰', '丽', '霞', '雪', '芳', '思'), cast(ceil(rand()*10) as int))
           ) name;

--rand()可以随机生成0到1之间的小数。
select rand() as n;
--随机生成0到10之间的小数。
select rand()*10 as n;
--随机生成1到10之间的整数。
select ceil(rand()*10) as n;

select elt(ceil(rand()*2), 'M', 'F') as sex;
select elt(ceil(rand()*4), 10, 15, 20, 30) ppp;

select floor(rand() * (70 - ppp - 18) + 18) age_buy
from (select elt(ceil(rand()*4), 10, 15, 20, 30) ppp) a;

select date_sub(current_date, round(rand() * (20 * 365))) buy_datetime;

drop table if exists cache_policy_client0;
cache table cache_policy_client0 as
select name,
       sex,
       ppp,
       case when ( area_id>=422 and area_id<=446) or ( area_id>=386 and area_id<=391)  then
                if(area_id%3==0,area_id,ceil(rand() * 460))
           else area_id
       end area_id,
       floor(rand() * (70 - ppp - 18) + 18)              age_buy,
       date_sub(current_date, ceil(rand() * (10 * 365))) buy_datetime,
       concat(area_id,'-',row_number() over (partition by area_id order by 1)) user_id,
       '相伴一生护理保险' insur_name,
       8866 insur_code
from (
    select concat(
                   element_at(array('张', '李', '王', '赵', '孙', '陈', '刘', '郭', '彭', '杨'), cast(ceil(rand() * 10) as int)),
                   element_at(array('平', '强', '华', '龙', '杰', '丽', '霞', '雪', '芳', '思'), cast(ceil(rand() * 10) as int))
               )                                 name,
           elt(ceil(rand() * 2), 'M', 'F')       sex,
           cast(elt(ceil(rand() * 4), 10, 15, 20, 30) as smallint) ppp,
           case when rad>0 and rad <0.05 then ceil(rand()*18) --北京
                when rad>0.05 and rad <0.08 then ceil(19+20*rand())--广东
                when rad>0.08 and rad <0.11 then 20--广州
                when rad>0.11 and rad <0.14 then 22--深圳
                when rad>0.14 and rad <0.17 then ceil(88+18*rand())--上海
                when rad>0.17 and rad <0.21 then ceil(57+12*rand())--江苏
                else ceil(rand() * 460) --其他
               end                    area_id
    from (select rand() rad, explode(array_repeat(1, 100000)) id) t
) a;

select area_id,count(1) from cache_policy_client0 group by area_id order by count(1) desc  limit 20;
--依据投保时间，继续生成保单号，和生日。关联地区信息。
drop table if exists cache_policy_client1;
cache table cache_policy_client1 as
select concat('P',
              repeat(0, 9 - length(row_number() over (order by a.buy_datetime))),
              row_number() over (order by a.buy_datetime)) pol_no,
       a.user_id,
       a.name,
       a.sex,
       a.ppp,
       date_sub(a.buy_datetime,(a.age_buy+rand())*365) birthday,
       a.age_buy,
       a.buy_datetime,
       a.insur_name,
       a.insur_code,
       b.province,
       b.city,
       b.direction,
       if(substr(user_id, length(user_id)-2, 2) in ('16'),10000000,
          elt(ceil(rand() * 3),100000,200000,300000)) income
from cache_policy_client0 a
join area b on a.area_id=b.id ;

--生成退保记录表
drop table policy_surrender;
--create table policy_surrender as

insert overwrite table insurance_ods.policy_surrender select * from insurance_ods.policy_surrender limit 1;
insert overwrite table policy_surrender
select pol_no,
       user_id,
       buy_datetime,
       keep_days,
       date_add(buy_datetime, keep_days) elapse_date
from (select pol_no,
             user_id,
             buy_datetime,
             ceil(rand() * datediff(current_date, buy_datetime)) keep_days
      from cache_policy_client1
      where substr(user_id, length(user_id)-2, 2) in ('15')
        and date_add(buy_datetime, cast(ppp as int) * 365) > current_date) a;

select count(1) from policy_surrender;

--依据退保记录，继续生成保单状态。

drop table cache_policy_client2;
cache table cache_policy_client2 as
select c.pol_no,
       c.user_id,
       name,
       concat(substr(ceil(rand()*99999999),0,6),replace(birthday,'-',''),substr(ceil(rand()*99999999),0,4)) id_card,
       substr(ceil((1+rand())*99999999999),0,11) phone,
       sex,
       ppp,
       birthday,
       age_buy,
       c.buy_datetime,
       insur_name,
       insur_code,
       province,
       city,
       direction,
       if(s.user_id is null, 1, 0) pol_flag,
       s.elapse_date,
       income
from cache_policy_client1  c
left join policy_surrender s on c.user_id = s.user_id
;

select province,
       count(user_id)
from cache_policy_client2
group by province
order by count(1) desc
limit 20;

select *
from cache_policy_client2
limit 4;

--drop table if exists policy_client;
insert overwrite table policy_client
select user_id,
       name,
       id_card,
       phone,
       sex,
       birthday,
       province,
       city,
       direction,
       income
from cache_policy_client2;

select count(1) from policy_client_old;
select * from policy_client_old limit 4;

set hive.warehouse.subdir.inherit.perms;

insert overwrite table insurance_ods.policy_benefit
select pol_no,
       user_id,
       ppp,
       age_buy,
       buy_datetime,
       insur_name,
       insur_code,
       pol_flag,
       elapse_date
from cache_policy_client2;

--需要生成一个保单号表。和客户信息表。
--理赔信息表。

insert overwrite table claim_info
select pol_no,
       user_id,
       buy_datetime,
       insur_code,
       claim_date,
       claim_item,
       case claim_item
           when 'sgbxj' then
                   prem*least(cast(c.ppp as int), ceil(datediff(claim_date, buy_datetime) / 365))
           when 'scbxj0' then
                   prem*least(cast(c.ppp as int), ceil(datediff(claim_date, buy_datetime) / 365))
           when 'scbxj1' then 10000
           when 'scbxj2' then 10000
           else 1000
           --暂时没考虑关爱养老金
           end claim_mnt
from (select pol_no,
             c.user_id,
             c.buy_datetime,
             c.insur_code,
             ppp,
             age_buy,
             sex,
             date_add(buy_datetime, ceil(rand() * (datediff(current_date, buy_datetime))))    claim_date,
             elt(ceil(rand() * 6), 'sgbxj', 'scbxj0', 'scbxj1', 'scbxj2', 'cqhlbxj', 'gaylj') claim_item
      from policy_benefit c
join policy_client t on c.user_id=t.user_id
where replace(pol_no,'P','')%30=0) c
left join prem_std_real s
          on c.age_buy = s.age_buy
              and c.ppp = s.ppp
              and c.sex = s.sex
;

select count(1) from claim_info;
select * from claim_info limit 6;