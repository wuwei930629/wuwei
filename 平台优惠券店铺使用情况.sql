select
	t1.ds,
	t1.template_id,
	t2.title,
	t1.shop_id,
	t3.shop_name,
	t3.cname1,
	t2.type_rule,
	t2.facevalue,
	t2.max_num,
	t2.create_num,
	t2.start_time,
	t2.end_time,
	t1.count_coupon,
	t1.count_user,
	t1.count_sn,
	t1.gmv,
	t1.fee
from
(
	select
		a1.ds,
		a1.template_id,
		a2.shop_id,
		count(distinct a1.coupon_sn) as count_coupon,
		count(distinct a2.user_id) as count_user,
		count(a1.order_sn) as count_sn,
		sum(a2.total_fee) as gmv,
		sum(a2.pay_fee) as fee
	from
	(
		select
			ds,
			template_id,
			shop_id,
			coupon_sn,
			order_sn
		from
			cc_order_coupon_paytime
		where
			ds>=20171207
			and
			template_id in(501048,501051,501053,501058,501059,501061,501080,501083,501089,501094,501099,501101)
	)a1
	left join
	(
		select
			order_sn,
			shop_id,
			pay_fee,
			total_fee,
			user_id
		from
			cc_order_user_pay_time
		where
			ds>=20171207
	)a2
	on a1.order_sn=a2.order_sn
	group by
		a1.ds,
		a1.template_id,
		a2.shop_id
)t1
left join
(
	select
		id,
		title,
		type_rule,
		facevalue,
		max_num,
		create_num,
		from_unixtime(start_time,'yyyyMMdd') as start_time,
		from_unixtime(end_time,'yyyyMMdd') as end_time
	from
		cc_coupon_temp
 	where
  		ds=20171211
)t2
on t1.template_id=t2.id
left join
(
	select
		shop_id,
		shop_name,
		cname1
	from
		data.cc_mid_shop_cname
	where
		ds=20171211
)t3
on t1.shop_id=t3.shop_id