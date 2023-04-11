
# 一、数据预处理

# 1.增加新列date_time（datetime）,dates（char，年⽉⽇），便于后续时间维度分析；
-- 增加新列date_time、dates
alter table o_retailers_trade_user add column date_time datetime null;
update o_retailers_trade_user
set date_time =str_to_date(time,'%Y-%m-%d %H') ;
-- %H可以表示0-23；%h表示0-12
alter table o_retailers_trade_user add column dates char(10) null;
update o_retailers_trade_user
set dates=date(date_time);
desc o_retailers_trade_user;
select * from o_retailers_trade_user limit 5;


# 2.重复值处理：创建新表a，并插⼊5W条⽆重复数据

-- 创建新表a，并插入5W条数据。
create table temp_trade like o_retailers_trade_user;
insert into temp_trade select distinct * from o_retailers_trade_user limit 50000;


-- 二、指标体系建设

# 1.用户指标体系


/*
 需求：uv、pv、浏览深度（按日）统计 
 pv：统计behavior_type=1的记录数，需要按日统计（分组）
 uv: 统计distinct user_id 的数量，需要按日统计（分组）
 浏览深度：pv/uv
*/
-- pv 进行cout时候，如果behavior_type=1进行计算，如果不是，不进行计算
select
 dates,
 count( distinct user_id ) as 'uv',
 count( if(behavior_type=1,user_id,null)) as 'pv',
 count( if(behavior_type=1,user_id,null))/count( distinct user_id ) as
'pv/uv'
from temp_trade
group by dates;


-- 用户留存
with temp_table_trades as
(select 
		a.dates,
    count(distinct b.user_id) as device_v，
		count(distinct if(datediff(b.dates,a.dates)=0,b.user_id,null)) as device_v_remain0,
		count(distinct if(datediff(b.dates,a.dates)=1,b.user_id,null)) as device_v_remain1,
		count(distinct if(datediff(b.dates,a.dates)=2,b.user_id,null)) as device_v_remain2,
		count(distinct if(datediff(b.dates,a.dates)=3,b.user_id,null)) as device_v_remain3,
		count(distinct if(datediff(b.dates,a.dates)=4,b.user_id,null)) as device_v_remain4,
		count(distinct if(datediff(b.dates,a.dates)=5,b.user_id,null)) as device_v_remain5,
		count(distinct if(datediff(b.dates,a.dates)=6,b.user_id,null)) as device_v_remain6,
		count(distinct if(datediff(b.dates,a.dates)=7,b.user_id,null)) as device_v_remain7,
		count(distinct if(datediff(b.dates,a.dates)=15,b.user_id,null)) as device_v_remain15,
		count(distinct if(datediff(b.dates,a.dates)=30,b.user_id,null)) as device_v_remain30
from
 (select
		user_id,
		dates
 from temp_trade
 group by user_id,dates ) a
left join
(
 select
		dates,
		user_id
 from temp_trade
 GROUP BY dates,user_id
) b 
on a.user_id = b.user_id
where b.dates >= a.dates
group by a.dates)


select 
		dates, 
		device_v_remain0,
		concat(cast((device_v_remain1/device_v_remain0)*100 as DECIMAL(18,2)),'%') as 'day_1%',
		concat(cast((device_v_remain2/device_v_remain0)*100 as DECIMAL(18,2)),'%') as 'day_2%',
		concat(cast((device_v_remain3/device_v_remain0)*100 as DECIMAL(18,2)),'%') as 'day_3%',
		concat(cast((device_v_remain4/device_v_remain0)*100 as DECIMAL(18,2)),'%') as 'day_4%',
		concat(cast((device_v_remain5/device_v_remain0)*100 as DECIMAL(18,2)),'%') as 'day_5%',
		concat(cast((device_v_remain6/device_v_remain0)*100 as DECIMAL(18,2)),'%') as 'day_6%',
		concat(cast((device_v_remain7/device_v_remain0)*100 as DECIMAL(18,2)),'%') as 'day_7%',
		concat(cast((device_v_remain15/device_v_remain0)*100 as DECIMAL(18,2)),'%') as 'day_15%',
		concat(cast((device_v_remain30/device_v_remain0)*100 as DECIMAL(18,2)),'%') as 'day_30%'
from temp_table_trades;

# RFM模型分析
-- RFM模型
-- 1.建立r视图，将近期购买时间提取到R临时表中
drop view if EXISTS user_recency;
create view user_Recency as
select 
		user_id ,
		max(dates) as rec_buy_time
from temp_trade
where behavior_type='2'
group by user_id
order by rec_buy_time desc;

-- 2.建立R等级划分视图：将客户近期购买时间进⾏等级划分，越接近2019-12-18号R越大；
drop view if exists r_clevel;
create view r_clevel as
select 
		user_id,
		rec_buy_time,
		datediff('2019-12-18',rec_buy_time) as recen_num,
		(case
		 when datediff('2019-12-18',rec_buy_time)<=2 then 5
		 when datediff('2019-12-18',rec_buy_time)<=4 then 4
		 when datediff('2019-12-18',rec_buy_time)<=6 then 3
		 when datediff('2019-12-18',rec_buy_time)<=8 then 2 else 1 end) as r_value
from user_Recency;

-- 1.建立F视图
create view frenq_value as
select 
		user_id ,
		count(user_id) as buy_frenq
from temp_trade
where behavior_type='2'
group by user_id;

-- 2.建立F等级划分
create view f_clevel as
select 
		user_id,
		buy_frenq,
		(case when buy_frenq<=2 then 1
		 when buy_frenq<=4 then 2
		 when buy_frenq<=6 then 3
		 when buy_frenq<=8 then 4
		else 5 end) as 'f_values'
from frenq_value;

select * from f_clevel;

-- 将用户整合
-- 1.R平均值
SELECT avg(r_value) as 'r_avg' FROM r_clevel; -- 2.7939
-- 2.F平均值
select avg(f_values) as 'f_avg' from f_clevel; -- 2.2606
-- 3.用户八大类等级划分，由于该数据没有M值，故只建立了4个分类
drop view if exists RFM_inall;
create view RFM_inall as
select 
		a.*,
		b.f_values,
		(case
		 when a.r_value>2.7939 and b.f_values>2.2606 then '重要⾼价值客户'
		 when a.r_value<2.7939 and b.f_values>2.2606 then '重要唤回客户'
		 when a.r_value>2.7939 and b.f_values<2.2606 then '重要深耕客户'
		 when a.r_value<2.7939 and b.f_values<2.2606 then '重要挽留客户' END) as user_class
from r_clevel a, f_clevel b
where a.user_id=b.user_id;

SELECT count(user_id) as user_v,user_class from RFM_inall GROUP BY user_class;




# 2.商品指标体系

-- 商品的点击量 收藏量 加购量 购买次数 购买转化
select * from temp_trade;
select item_id,
sum(case when behavior_type=1 then 1 else 0 end) as'pv',
sum(case when behavior_type=4 then 1 else 0 end) as'fav',
sum(case when behavior_type=3 then 1 else 0 end) as'cart',
sum(case when behavior_type=2 then 1 else 0 end) as'buy',
count(distinct case when behavior_type=2 then user_id else null end)/count(distinct user_id) as buy_rate
from temp_trade
group by item_id
order by buy desc;


select item_category,
sum(case when behavior_type=1 then 1 else 0 end) as'pv',
sum(case when behavior_type=4 then 1 else 0 end) as'fav',
sum(case when behavior_type=3 then 1 else 0 end) as'cart',
sum(case when behavior_type=2 then 1 else 0 end) as'buy',
count(distinct case when behavior_type=2 then user_id else null end)/count(distinct user_id) as buy_rate
from temp_trade
group by item_category
order by buy desc;


# 3.平台指标体系

-- 每日的分析(1-4，分别表示点击pv、购买buy、加购物⻋cart、喜欢fav)
select 
	dates,
	count(1) as '每⽇的总数',
	sum(case when behavior_type=1 then 1 else 0 end) as'pv',
	sum(case when behavior_type=2 then 1 else 0 end) as'buy',
	sum(case when behavior_type=3 then 1 else 0 end) as'cart',
	sum(case when behavior_type=4 then 1 else 0 end) as'fav',
	count(distinct case when behavior_type=2 then user_id else null end)/count(distinct user_id) as buy_rate
from temp_trade
group by dates;


-- 行为路径分析
-- 行为路径组建基础视图
drop view product_user_way;
create view product_user_way as 
select
	a.*
from
 (
 select
	user_id,
	item_id,
	lag ( behavior_type, 4 ) over ( partition by user_id, item_id order by date_time ) lag_4,
	lag ( behavior_type, 3 ) over ( partition by user_id, item_id order by date_time ) lag_3,
	lag ( behavior_type, 2 ) over ( partition by user_id, item_id order by date_time ) lag_2,
	lag ( behavior_type ) over ( partition by user_id, item_id order by date_time ) lag_1,
	behavior_type,
	rank ( ) over ( partition by user_id, item_id order by date_time desc ) as rank_dn # 倒数第几个行为
 from
 temp_trade
 ) a
where a.rank_dn = 1 and behavior_type = 2
 
-- 查询该路径下有多少购买用户数
select
 concat(ifnull( lag_4, '空' ), "-", ifnull( lag_3, '空' ), "-", ifnull( lag_2, '空' ), "-", ifnull( lag_1, '空' ), "-", behavior_type) as user_way,
 count( distinct user_id ) as user_count -- 该路径下购买用户数
from product_user_way
group by concat(ifnull( lag_4, '空' ), "-", ifnull( lag_3, '空' ), "-", ifnull( lag_2, '空' ), "-", ifnull( lag_1, '空' ), "-", behavior_type);






























