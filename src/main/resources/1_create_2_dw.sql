--开启spark-sql客户端，将下面的代码粘贴到spark-sql中运行。
drop database  if exists insurance_dw cascade ;
create database insurance_dw;
use insurance_dw;

drop table if exists prem_src;
create table prem_src
(
    age_buy       smallint comment '投保年龄',
    nursing_age   smallint comment '长期护理保险金给付期满年龄',
    sex           string comment '性别',
    t_age         smallint comment '满期年龄(Terminate Age)',
    ppp           smallint comment '交费期间(Premuim Payment Period PPP)',
    bpp           smallint comment '保险期间(BPP)',
    interest_rate decimal(6, 4)  comment '预定利息率(Interest Rate PREM&RSV)',
    sa            decimal(12, 2) comment '基本保险金额(Baisc Sum Assured)',
    policy_year   smallint comment '保单年度',
    age           smallint comment '保单年度对应的年龄',
    qx            decimal(17, 12) comment '死亡率',
    kx            decimal(17, 12) comment '残疾死亡占死亡的比例',
    qx_d          decimal(17, 12) comment '扣除残疾的死亡率',
    qx_ci         decimal(17, 12) comment '残疾率',
    dx_d          decimal(17, 12) comment '',
    dx_ci         decimal(17, 12) comment '',
    lx            decimal(17, 12) comment '有效保单数',
    lx_d          decimal(17, 12) comment '健康人数',
    cx            decimal(17, 12) comment '当期发生该事件的概率，如下指的是死亡发生概率',
    cx_           decimal(17, 12) comment '对Cx做调整，不精确的话，可以不做',
    ci_cx         decimal(17, 12) comment '当期发生重疾的概率',
    ci_cx_        decimal(17, 12) comment '当期发生重疾的概率，调整',
    dx            decimal(17, 12) comment '有效保单生存因子',
    dx_d_         decimal(17, 12) comment '健康人数生存因子',
    ppp_          smallint comment '是否在缴费期间，1-是，0-否',
    bpp_          smallint comment '是否在保险期间，1-是，0-否',
    expense       decimal(17, 12) comment '附加费用率',
    db1           decimal(17, 12) comment '残疾给付',
    db2_factor    decimal(17, 12) comment '长期护理保险金给付因子',
    db2           decimal(17, 12) comment '长期护理保险金',
    db3           decimal(17, 12) comment '养老关爱金',
    db4           decimal(5, 2) comment '身故给付保险金',
    db5           decimal(17, 12) comment '豁免保费因子'
) comment '保费因子表（到每个保单年度）'
    row format delimited fields terminated by '\t';


drop table if exists prem_std;
create table prem_std
(
    age_buy smallint comment '年投保龄',
    sex     string comment '性别',
    ppp     smallint comment '缴费期',
    bpp     string comment '保障期',
    prem    decimal(14, 6) comment '每期交的保费'
) comment '标准保费结果表' row format delimited
    fields terminated by '\t';


drop table if exists cv_src;
create table cv_src(
    age_buy       smallint comment '年投保龄',
    nursing_age   smallint comment '长期护理保险金给付期满年龄',
    sex           string comment '性别',
    t_age         smallint comment '满期年龄(Terminate Age)',
    ppp           smallint comment '交费期间(Premuim Payment Period PPP)',
    bpp           smallint comment '保险期间(BPP)',
    interest_rate_cv decimal(6, 4) comment '现金价值预定利息率（Interest Rate CV）',
    sa            decimal(12, 2) comment '基本保险金额(Baisc Sum Assured)',
    policy_year   smallint comment '保单年度',
    age           smallint comment '保单年度对应的年龄',
    qx            decimal(8, 7) comment '死亡率',
    kx            decimal(8, 7) comment '残疾死亡占死亡的比例',
    qx_d          decimal(8, 7) comment '扣除残疾的死亡率',
    qx_ci         decimal(8, 7) comment '残疾率',
    dx_d          decimal(8, 7) comment '',
    dx_ci         decimal(8, 7) comment '',
    lx            decimal(8, 7) comment '有效保单数',
    lx_d          decimal(8, 7) comment '健康人数',
    cx            decimal(8, 7) comment '当期发生该事件的概率，如下指的是死亡发生概率',
    cx_           decimal(8, 7) comment '对Cx做调整，不精确的话，可以不做',
    ci_cx         decimal(8, 7) comment '当期发生重疾的概率',
    ci_cx_        decimal(8, 7) comment '当期发生重疾的概率，调整',
    dx            decimal(8, 7) comment '有效保单生存因子',
    dx_d_         decimal(8, 7) comment '健康人数生存因子',
    ppp_          smallint comment '是否在缴费期间，1-是，0-否',
    bpp_          smallint comment '是否在保险期间，1-是，0-否',
    expense       decimal(8, 7) comment '附加费用率',
    db1           decimal(12, 2) comment '残疾给付',
    db2_factor    decimal(8, 7) comment '长期护理保险金给付因子',
    db2           decimal(12, 2) comment '长期护理保险金',
    db3           decimal(12, 2) comment '养老关爱金',
    db4           decimal(12, 2) comment '身故给付保险金',
    db5           decimal(12, 2) comment '豁免保费因子',
    np_         DECIMAL(12, 2) comment '净保费',
    pvnp        DECIMAL(17, 7) comment '净保费现值',
    pvdb1       DECIMAL(17, 7) comment '',
    pvdb2       DECIMAL(17, 7) comment '',
    pvdb3       DECIMAL(17, 7) comment '',
    pvdb4       DECIMAL(17, 7) comment '',
    pvdb5       DECIMAL(17, 7) comment '',
    pvr         DECIMAL(17, 7) comment '保单价值准备金',
    rt          DECIMAL(6, 3) comment '',
    np          DECIMAL(17, 7) comment '修匀净保费',
    sur_ben     DECIMAL(17, 7) comment '生存金',
    cv_1a       DECIMAL(17, 7) comment '现金价值年末（生存给付前）',
    cv_1b       DECIMAL(17, 7) comment '现金价值年末（生存给付后）',
    cv_2        DECIMAL(17, 7) comment '现金价值年中'
)comment '现金价值表（到每个保单年度）' row format delimited
    fields terminated by ',';

drop table if exists prem_cv;
create table prem_cv
(
    age_buy smallint comment '年投保龄',
    sex     string comment '性别',
    ppp     smallint comment '缴费期间',
    prem_cv      decimal(15, 7) comment '保单价值准备金毛保险费(Preuim)'
)comment '保单价值准备金毛保险费表' row format delimited
    fields terminated by '\t';

drop table if exists rsv_src;
create table rsv_src
(
    age_buy       smallint comment '投保年龄',
    nursing_age   smallint comment '长期护理保险金给付期满年龄',
    sex           string comment '性别',
    t_age         smallint comment '满期年龄(Terminate Age)',
    ppp           smallint comment '交费期间(Premuim Payment Period PPP)',
    bpp           smallint comment '保险期间(BPP)',
    interest_rate decimal(6, 4)  comment '预定利息率(Interest Rate PREM&RSV)',
    sa            decimal(12, 2) comment '基本保险金额(Baisc Sum Assured)',
    policy_year   smallint comment '保单年度',
    age           smallint comment '保单年度对应的年龄',
    qx            decimal(8,7) comment '死亡率',
    kx            decimal(8,7) comment '残疾死亡占死亡的比例',
    qx_d          decimal(8,7) comment '扣除残疾的死亡率',
    qx_ci         decimal(8,7) comment '残疾率',
    dx_d          decimal(8,7) comment '',
    dx_ci         decimal(8,7) comment '',
    lx            decimal(8,7) comment '有效保单数',
    lx_d          decimal(8,7) comment '健康人数',
    cx            decimal(8,7) comment '当期发生该事件的概率，如下指的是死亡发生概率',
    cx_           decimal(8,7) comment '对Cx做调整，不精确的话，可以不做',
    ci_cx         decimal(8,7) comment '当期发生重疾的概率',
    ci_cx_        decimal(8,7) comment '当期发生重疾的概率，调整',
    dx            decimal(8,7) comment '有效保单生存因子',
    dx_d_         decimal(8,7) comment '健康人数生存因子',
    ppp_          smallint comment '是否在缴费期间，1-是，0-否',
    bpp_          smallint comment '是否在保险期间，1-是，0-否',
    db1           decimal(12, 2) comment '残疾给付',
    db2_factor    decimal(8, 7) comment '长期护理保险金给付因子',
    db2           decimal(12, 2) comment '长期护理保险金',
    db3           decimal(12, 2) comment '养老关爱金',
    db4           decimal(12, 2) comment '身故给付保险金',
    db5           decimal(12, 2) comment '豁免保费因子',
    np_           decimal(12, 2) comment '修正纯保费',
    pvnp          decimal(17, 7) comment '修正纯保费现值',
    pvdb1         decimal(17, 7) comment '',
    pvdb2         decimal(17, 7) comment '',
    pvdb3         decimal(17, 7) comment '',
    pvdb4         decimal(17, 7) comment '',
    pvdb5         decimal(17, 7) comment '',
    prem_rsv      decimal(17, 7) comment '保险费(Preuim)',
    alpha         decimal(17, 7) comment '修正纯保费首年',
    beta          decimal(17, 7) comment '修正纯保费续年',
    rsv1          decimal(17, 7) comment '准备金年末',
    rsv2          decimal(17, 7) comment '准备金年初（未加当年初纯保费）',
    rsv1_re       decimal(17, 7) comment '修正责任准备金年末',
    rsv2_re       decimal(17, 7) comment '修正责任准备金年初(未加当年初纯保费）'
)comment '准备金表（到每个保单年度）' row format delimited
    fields terminated by ',';
