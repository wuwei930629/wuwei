select
	p1.ds,
	p1.source,
	p1.fee,
	p1.count_sn,
	p2.ipv_uv
from
(
	select
		t1.ds,
		t1.source,
		sum(t1.pay_fee) as fee,
		sum(t1.count_sn) as count_sn
	from
	(
		select
		ds,
			(case
		when deal_way in(1,101,15000041,15000042,15000043,15000044,15000034,15000037,15000141,15000142,15000143,15000144,15000134,15000137) then '九块九'
		when deal_way in(4,104) then '品牌团'
		when deal_way in(43,143) then '品牌团'
		when deal_way in(44,144) then '品牌团'
		when deal_way >30000000 and deal_way <40000000 then '品牌团'
		when deal_way in(30,130,77,177,15310067,15310167,15309967) then '抢购'
		when deal_way >40000000 and deal_way < 50000000 then '抢购'
		when deal_way in (7,107,98,198) then '搜索'
		when deal_way in (3,103,97,197) then '分类'
		when deal_way in (10,110,26,126,28,128,24,124,25,125,45,145) then '店铺首页'
   		when deal_way in (15092036,15092136,15091936) then '良品'
    	when deal_way in (90,91,92,93,94,95,96,99,190,191,192,193,194,195,196,199) then '推荐'
   		when deal_way in (15287069,15299064,15322042,15287169,15299164,15322142) then '品牌折扣'
		when deal_way >80000000 and deal_way <90000000 then '潮青年'
		else '其他' end) as source,
			sum(pay_fee) as pay_fee,
			count(order_sn) as count_sn
		from
			cc_order_user_pay_time
		where
			shop_id not in (148,9206,1378) 
			and 
			deal_way not in(14,114,80,180,85,185,87,187)
			and
			source_channel !=2
			and
			ds >= '${bizdate}'
		group by
			ds,
			deal_way
	)t1
	group by
		t1.ds,
		t1.source
)p1
left join
(
	select
		t2.ds,
		t2.source,
		sum(t2.ipv_uv) as ipv_uv
	from
	(
		select
			ds,
			(case
			when track_id in(1,101,15000041,15000042,15000043,15000044,15000034,15000037,15000141,15000142,15000143,15000144,15000134,15000137) then '九块九'
			when track_id in(4,104) then '品牌团'
			when track_id in(43,143) then '品牌团'
			when track_id in(44,144) then '品牌团'
			when track_id >30000000 and track_id <40000000 then '品牌团'
			when track_id in(30,130,77,177,15310067,15310167,15309967) then '抢购'
			when track_id >40000000 and track_id < 50000000 then '抢购'
			when track_id in (7,107,98,198) then '搜索'
			when track_id in (3,103,97,197) then '分类'
			when track_id in (10,110,26,126,28,128,24,124,25,125,45,145) then '店铺首页'
	   		when track_id in (15092036,15092136,15091936) then '良品'
	    	when track_id in (90,91,92,93,94,95,96,99,190,191,192,193,194,195,196,199) then '推荐'
	   		when track_id in (15287069,15299064,15322042,15287169,15299164,15322142) then '品牌折扣'
			when track_id >9000000 and track_id <30000000 then '活动页'
			when track_id >80000000 and track_id <90000000 then '潮青年'
			else '其他' end) as source,
			sum(uv) as ipv_uv
		from
			data.cc_dm_count_shop_product_source
		where
			shop_id not in (148,9206,1378) 
			and 
			track_id not in(14,114,80,180,85,185,87,187)
			and
			ds >= '${bizdate}'
		group by
			ds,
			track_id
	)t2
	group by
		t2.ds,
		t2.source
)p2
on p1.ds=p2.ds and p1.source=p2.source