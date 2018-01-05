select
	t1.ds,
	t1.dau,
	t4.pay_fee,
	t1.ipv_uv,
	(t4.count_sn/t1.ipv_uv) as rate_uv,
	t4.count_sn,
	(t4.pay_fee/t4.count_sn) as price,
	t2.count_shop,
	t3.count_pd
from
(
	select
		ds,
		dau,
		product_uv as ipv_uv
	from
		report.cc_rpt_exc_holistic_gmv_stat
	where
		ds >= '${bizdate}'
)t1
left join
(
	select
		ds,
		count(distinct id) as count_shop
	from
		cc_shop
	where
		ds >= '${bizdate}'
		and
		status =0
	group by	
		ds
)t2
on t1.ds=t2.ds
left join
(
	select
		a1.ds,
		count(distinct a1.product_id) as count_pd
    from
	(
		select
			from_unixtime(ctime,'yyyMMdd') as ds,
			product_id		
		from
			cc_ods_fs_product
		where
			ctime>=1509465600
	)a1
	group by
		a1.ds
)t3
on t1.ds=t3.ds
left join
(
	select
		ds,
		sum(pay_fee) as pay_fee,
		count(order_sn) as count_sn
	from
		cc_order_user_pay_time
	where
		ds >= '${bizdate}'
		and
		shop_id not in (148,9206,1378) 
		and 
		deal_way not in(14,114,80,180,85,185,87,187)
		and
		source_channel !=2
	group by
		ds
)t4
on t1.ds=t4.ds
