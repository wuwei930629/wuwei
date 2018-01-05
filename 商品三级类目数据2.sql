insert overwrite table tmp.tmp_text_wuwei_category_mask
select
	p1.time,
	p1.c2,
	p1.c3,
	p1.category,
	(case
	when p2.rate_fee_category >=0.001 then (0.2*p1.rate_detail_category_hour/p2.rate_detail_category)+(0.8*p1.rate_fee_category_hour/p2.rate_fee_category)
	else 1 end
	)as mask
from
(
	select
		t1.time,
		t1.c2,
		t1.c3,
		t1.category,
		(t1.detail_count/t2.detail_hour) as rate_detail_category_hour,
		(t1.fee/t2.fee_hour) as rate_fee_category_hour
	from
	(
		select
			time,
			c2,
			c3,
			category,
			count_detail as detail_count,
			fee
		from
			tmp_text_wuwei_category_data
	)t1
	left join
	(
		select
			time,
			sum(count_detail) as detail_hour,
			sum(fee) as fee_hour
		from
			tmp_text_wuwei_category_data
		group by
			time
	)t2
	on t1.time=t2.time
)p1
left join
(	
	select
		category,
		(t3.detail_category/t4.detail) as rate_detail_category,
		(t3.fee_category/t4.pay_fee) as rate_fee_category
	from
	(
		select
			category,
			1 as tab,
			sum(count_detail) detail_category,
			sum(fee) as fee_category
		from
			tmp_text_wuwei_category_data
		group by
			category
	)t3
	left join
	(
		select
 			1 as tab,
 			sum(count_detail) detail,
			sum(fee) as pay_fee
 		from
 			tmp_text_wuwei_category_data
	)t4
	on t3.tab=t4.tab
)p2
on p1.category = p2.category
