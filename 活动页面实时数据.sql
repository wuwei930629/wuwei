
	select
	t1.page_id,
	t3.title,
	CASE WHEN t2.pv IS NULL THEN 0 ELSE t2.pv END,
	CASE WHEN t2.uv IS NULL THEN 0 ELSE t2.uv END,
	CASE WHEN t1.count_product IS NULL THEN 0 ELSE t1.count_product END,
	CASE WHEN t1.ipv_uv IS NULL THEN 0 ELSE t1.ipv_uv END,
	CASE WHEN t1.count_pay IS NULL THEN 0 ELSE t1.count_pay END,
	CASE WHEN t1.fee IS NULL THEN 0 ELSE t1.fee END,
	CASE WHEN (t1.fee/t1.count_pay) IS NULL THEN 0 ELSE (t1.fee/t1.count_pay) END,
    CASE WHEN (t1.fee/t2.uv) IS NULL THEN 0.0 ELSE (t1.fee/t2.uv)  END
from
(
	select
		(p1.track_id%100+int(substr(p1.track_id,3)/1000)*100) as page_id,
	   	sum(p1.ipv_uv) as ipv_uv,
	   	sum(p1.count_product) as count_product,
	   	sum(p1.fee) as fee,
	   	sum(p1.count_pay) as count_pay
	from
	(
		select
			q1.track_id,
			sum(q1.ipv_uv) as ipv_uv,
			sum(q1.count_product) as count_product,
			sum(q1.count_pay) as count_pay,
			sum(q1.fee) as fee
		from
		(select
			(case
			when substr(n1.deal_way,6,1) = 1 then int(n1.deal_way-100)
			else n1.deal_way end) as track_id,
			n2.ipv_uv,
			n2.count_product,
			n1.count_pay,
			n1.fee
		from
		(
			select
				deal_way,
				count(order_sn) as count_pay,
				sum(pay_fee) as fee
			from
				origin_common.cc_ods_order_user_pay_time_hourly 
			where
				ds>=20171207
				and
				deal_way >9000000
			group by
				deal_way
			
		)n1
		left join
		(
			select
				track_id,
				count(distinct user_id) as ipv_uv,
				count(distinct product_id) as count_product
			from
				origin_common.cc_ods_log_nginx_product_detail_hourly
			where
				ds>=20171207
				and
				track_id >9000000
			group by
				track_id
		)n2
		on n2.track_id=n1.deal_way
	)q1
		group by
			q1.track_id
	)p1
	group by
		(p1.track_id%100+int(substr(p1.track_id,3)/1000)*100) 
)t1
left join
(
	
	select
		a1.page_id,
		count(a1.ip_address) AS pv,
        count(DISTINCT a1.ip_address) AS uv
    from
    (
		select
			split(html_id,'_')[1] AS page_id,
			ip_address
		from
			tmp.cc_ods_log_wap_afp_hourly
		where
			ds>=20171207
	)a1
	GROUP BY
		a1.page_id
)t2
ON t1.page_id=t2.page_id
left join
(
	select
		id,
		title
	from
		origin_common.cc_ods_fs_afp_page
	where
		ds=20171206
)t3
on t1.page_id=t3.id
