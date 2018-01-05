insert overwrite table tmp.tmp_text_wuwei_category_data
	select
		p1.time,
		p1.c2,
		p1.c3,
		p1.category,
		p1.count_detail,
		p2.fee
	from
	(
		select
			t1.time,
			t1.c2,
			t1.c3,
			t1.category,
			t2.count_detail
		from
		(
				select
					time,
					category,
					c2,
					c3
				from
					tmp.tmp_text_wuwei_category_hour
		)t1
		left join
		(
			select
				a1.time,
				a2.category,
				sum(a1.uv) as count_detail
			from
			(
				select
					(case
					when hour in('23','00','01','02','03','04','05','06','07','08') then '23-9'
					when hour in('09','10') then '9-11'
					when hour in('11','12') then '11-13'
					when hour in('13','14') then '13-15'
					when hour in('15','16') then '15-17'
					when hour in('17','18') then '17-19'
					when hour in('19','20') then '19-21'
					when hour in('21','22') then '21-23'
		            else 0 end
					) as time,
					product_id,
					uv
				from
					data.cc_dm_one_hour_nginx_shop_product_source_hourly 
				where
					ds >= '${begin_date}'
					and
					ds <= '${end_date}'
					and
					track_id in(7,107,98,198)
			)a1
			left join
			(
				select
					b1.product_id,
					(case
					when b2.c3 = '-100' then b2.c2 else b2.c3 end) as category
				from
					origin_common.cc_ods_fs_product b1
				join
					origin_common.cc_category_cascade b2
				on b1.cid=b2.last_cid
				where
					b2.ds = '${end_date}'
			)a2
			on a1.product_id=a2.product_id
			group by
				a1.time,
				a2.category
		)t2
		on t1.time = t2.time and t1.category = t2.category
	)p1
	left join
	(
		select
			t3.time,
			t3.c2,
			t3.c3,
			t3.category,
			t4.fee
		from
		(
				select
					time,
					c2,
					c3,
					category
				from
					tmp.tmp_text_wuwei_category_hour
		)t3
		left join
		(
			select
				a3.time,
				a4.category,
				sum(a3.product_discount_price * a3.product_count) as fee
			from
			(
				select
					(case
					when s1.hour in('23','00','01','02','03','04','05','06','07','08') then '23-9'
					when s1.hour in('09','10') then '9-11'
					when s1.hour in('11','12') then '11-13'
					when s1.hour in('13','14') then '13-15'
					when s1.hour in('15','16') then '15-17'
					when s1.hour in('17','18') then '17-19'
					when s1.hour in('19','20') then '19-21'
					when s1.hour in('21','22') then '21-23'
		            else 0 end
					) as time,
					s1.product_id,
					s1.product_discount_price,
					s1.product_count
				from
					origin_common.cc_ods_order_products_user_pay_time_hourly s1
				join
					origin_common.cc_order_user_pay_time s2
				on s1.order_sn=s2.order_sn
				where
					s1.ds >= '${begin_date}'
					and
					s1.ds <= '${end_date}'
					and
					s2.ds >= '${begin_date}'
					and
					s2.ds <= '${end_date}'
					and
					(s2.source_channel !=2 or s2.source_channel is null)
					and
			        s2.deal_way in(7,107,98,198)
			)a3
			left join
			(
				select
					b3.product_id,
					(case
					when b4.c3 = '-100' then b4.c2 else b4.c3 end) as category
				from
					origin_common.cc_ods_fs_product b3
				join
					origin_common.cc_category_cascade b4
				on b3.cid=b4.last_cid
				where
					b4.ds = '${end_date}'
			)a4
			on a3.product_id=a4.product_id
			group by
				a3.time,
				a4.category
		)t4
		on t3.time=t4.time and t3.category=t4.category
	)p2
	on p1.time=p2.time and p1.category=p2.category