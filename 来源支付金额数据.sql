select
	p1.source,
	sum(p1.fee),
	sum(p1.count_sn)
from
(
select
	(case
	when t1.deal_way in(1,101,15000041,15000042,15000043,15000044,15000034,15000037,15000141,15000142,15000143,15000144,15000134,15000137) then '九块九'
	when t1.deal_way in(4,104) then '品牌团'
	when t1.deal_way in(43,143) then '品牌团'
	when t1.deal_way in(44,144) then '品牌团'
	when t1.deal_way >30000000 and t1.deal_way <40000000 then '品牌团'
	when t1.deal_way in(30,130,77,177,15310067,15310167,15309967 ) then '抢购'
	when t1.deal_way >40000000 and t1.deal_way < 50000000 then '抢购'
	when t1.deal_way in (7,107,98,198) then '搜索'
	when t1.deal_way in (3,103,97,197) then '分类'
	when t1.deal_way in (10,110,26,126,28,128,24,124,25,125,45,145) then '店铺首页'
	when t1.deal_way in (11,111) then '收藏'
	when t1.deal_way in (5,105) then '购物车'
	when t1.deal_way >9000000 and t1.deal_way <30000000 then '活动页'
	else '其他' end) as source,
	t1.fee,
	t1.count_sn
from
(
	select
		deal_way,
		sum(pay_fee) as fee,
		count(order_sn) as count_sn
	from
		cc_order_user_pay_time
	where
		ds>=20161101
		and
		ds<20161201
		and
		shop_id not in(1378,9206,148)
	group by
		deal_way
)t1
)p1
group by 
	p1.source
