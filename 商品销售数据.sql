select
	p1.ds,
	p1.product_id,
	p4.cn_title,
	p2.c1_name,
	p2.c2_name,
	p2.c3_name,
	p1.shop_id,
	p3.shop_name,
	p3.cname1,
	p3.cname2,
	p1.count_sn,
	p1.count_user,
	p1.count_pd,
	p1.gmv,
	p1.fee,
	p1.ipv,
	p1.ipv_uv
from
(
select
	t1.ds,
	t1.product_id,
	t1.shop_id,
	t1.count_sn,
	t1.count_user,
	t1.count_pd,
	t1.gmv,
	t1.fee,
	t2.ipv,
	t2.ipv_uv
from
	(
	select
		a1.ds,
		a1.product_id,
		a2.shop_id,
		sum(a1.product_count * a1.product_discount_price) as fee,
		count(a1.order_sn) as count_sn,
		sum(a1.product_count) as count_pd,
		count(distinct a2.user_id) as count_user,
		sum(a2.total_fee) as gmv
	from
		(
		select
			ds,
			product_id,
			order_sn,
			product_count,
			product_discount_price
		from
			cc_order_products_user_pay_time
		where
			ds>=20171207
			and
			(source_channel&1=0 and source_channel&2=0 and source_channel&4=0) 

		)a1
		inner join
		(
		select
			order_sn,
			shop_id,
			user_id,
			total_fee
		from
			cc_order_user_pay_time
		where
			ds>=20171207
			and
        	shop_id not in(1378,148,9206)
       		and
        	source_channel !=2
        	and
        	deal_way not in(14,114,80,180,85,185,87,187)
		)a2
		on a1.order_sn=a2.order_sn
		group by
			a1.ds,
			a1.product_id,
			a2.shop_id
	)t1
	left join
	(
		select
			ds,
			product_id,
			sum(uv) as ipv_uv,
			sum(pv) as ipv
		from
          data.cc_dm_count_shop_product_source
        where
        	ds>=20171207
        group by
        	ds,
        	product_id
	)t2
	on t1.product_id=t2.product_id and t1.ds=t2.ds
)p1
left join
(
	select
		product_id,
		c1_name,
		c2_name,
		c3_name
	from
		data.cc_dw_fs_product_category
)p2
on p1.product_id=p2.product_id
left join
(
	select
		shop_id,
		shop_name,
		cname1,
		cname2
	from
		data.cc_mid_shop_cname
	where
		ds=20171211
)p3
on p1.shop_id=p3.shop_id
left join
(
	select
		product_id,
		cn_title
	from
		 cc_ods_fs_product
)p4
on p1.product_id=p4.product_id
