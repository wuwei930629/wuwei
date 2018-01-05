select
	p5.user_id,
	p5.category1,
	p6.phone_number
from
(
select
	p4.user_id,
	max(p4.category1) as category1
from
(
	select
	p3.category1,
	p3.user_id,
	dense_rank() over(partition by p3.user_id order by p3.score desc) as num 
from
(
select
	distinct p1.user_id,
	p1.category1,
	(p1.score+p2.mark) as score
from
(
	select
		b1.user_id,
		b1.category1,
		(case 
 		when b1.count_sn >=3 then 60
 		when b1.count_sn >=2 and b1.count_sn<3 then 40
 		when b1.count_sn >=1 and b1.count_sn<2 then 20
   		else 0 end) as score
	from
	(
		select
			a1.user_id,
			a3.category1,
			sum(a2.count_sn) as count_sn
		from
			(
				select
					user_id
				from 
					tmp.tmp_text_wuwei_20171205 
			)a1
			left join
			(
				select
					user_id,
					shop_id,
					count(order_sn) as count_sn
				from
					cc_order_user_pay_time
				where
						ds>=20171101
				        and
				        ds<20171201
						and
						source_channel !=2
				group by
					user_id,
					shop_id
			)a2
			on a1.user_id=a2.user_id
			left join
			(
				select
					shop_id,
					category1
				from
					cc_ods_fs_business_basic 
			)a3
			on a2.shop_id=a3.shop_id
			group by
				a1.user_id,
				a3.category1
	)b1
)p1
left join
(	
	select
		b2.user_id,
		b2.c1,
		(case 
 		when b2.count_sn >=10 then 40
 		when b2.count_sn >=9 and b2.count_sn<10 then 36
 		when b2.count_sn >=8 and b2.count_sn<9 then 32
 		when b2.count_sn >=7 and b2.count_sn<8 then 28
 		when b2.count_sn >=6 and b2.count_sn<7 then 24
 		when b2.count_sn >=5 and b2.count_sn<6 then 20
 		when b2.count_sn >=4 and b2.count_sn<5 then 16
 		when b2.count_sn >=3 and b2.count_sn<4 then 12
 		when b2.count_sn >=2 and b2.count_sn<3 then 8
 		when b2.count_sn >=1 and b2.count_sn<2 then 4
   		else 0 end) as mark
	from
	(
		select
			t1.user_id,
			t2.c1,
			count(distinct t1.product_id) as count_sn
		from
		(
			select
				distinct r1.user_id,
				r2.product_id
			from
			(
				select
					user_id
				from 
					tmp.tmp_text_wuwei_20171205 
			)r1
			left join
			(
				select
					user_id,
					product_id
				from
					cc_ods_log_nginx_product_detail_hourly
				where
					ds>=20171101
					and
					ds<20171201
			)r2
			on r1.user_id=r2.user_id
		)t1
		left join
		(
			select
				s1.product_id,
				s2.c1
			from
				cc_ods_fs_product s1
			join
				cc_category_cascade s2
			on s1.cid=s2.last_cid
		)t2
		on t1.product_id=t2.product_id
		group by
			t1.user_id,
			t2.c1
	)b2
)p2
on p1.user_id=p2.user_id and p1.category1=p2.c1
)p3
)p4
where
	p4.num=1
group by
	p4.user_id
)p5
left join
(
	select
		user_id,
		phone_number
	from
		cc_ods_fs_user
)p6
on p5.user_id=p6.user_id