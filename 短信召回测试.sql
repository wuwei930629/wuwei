select
	n4.user_id,
	n4.tab
from
(
	select
		n3.user_id,
		n3.tab,
		dense_rank() over(partition by n3.tab order by n3.number desc) as num
	from
	(
		select
			user_id,
			(
				case
				when n1.count_sn =0 and n1.count_click < n2.avg_click and n1.count_detail < n2.avg_detail and n1.count_add < n2.avg_add then 1
				when n1.count_sn =0 and n1.count_click >= n2.avg_click and n1.count_detail < n2.avg_detail and n1.count_add < n2.avg_add then 2
				when n1.count_sn =0 and n1.count_click < n2.avg_click and n1.count_detail >= n2.avg_detail and n1.count_add < n2.avg_add then 3
				when n1.count_sn =0 and n1.count_click < n2.avg_click and n1.count_detail < n2.avg_detail and n1.count_add >= n2.avg_add then 4
				when n1.count_sn =0 and n1.count_click >= n2.avg_click and n1.count_detail >= n2.avg_detail and n1.count_add < n2.avg_add then 5
				when n1.count_sn =0 and n1.count_click < n2.avg_click and n1.count_detail >= n2.avg_detail and n1.count_add >= n2.avg_add then 6
				when n1.count_sn =0 and n1.count_click >= n2.avg_click and n1.count_detail < n2.avg_detail and n1.count_add >= n2.avg_add then 7
				when n1.count_sn =0 and n1.count_click >= n2.avg_click and n1.count_detail >= n2.avg_detail and n1.count_add >= n2.avg_add then 8
				when n1.count_sn !=0 and n1.count_click < n2.avg_click and n1.count_detail < n2.avg_detail and n1.count_add < n2.avg_add then 9
				when n1.count_sn !=0 and n1.count_click >= n2.avg_click and n1.count_detail < n2.avg_detail and n1.count_add < n2.avg_add then 10
				when n1.count_sn !=0 and n1.count_click < n2.avg_click and n1.count_detail >= n2.avg_detail and n1.count_add < n2.avg_add then 11
				when n1.count_sn !=0 and n1.count_click < n2.avg_click and n1.count_detail < n2.avg_detail and n1.count_add >= n2.avg_add then 12
				when n1.count_sn !=0 and n1.count_click >= n2.avg_click and n1.count_detail >= n2.avg_detail and n1.count_add < n2.avg_add then 13
				when n1.count_sn !=0 and n1.count_click < n2.avg_click and n1.count_detail >= n2.avg_detail and n1.count_add >= n2.avg_add then 14
				when n1.count_sn !=0 and n1.count_click >= n2.avg_click and n1.count_detail < n2.avg_detail and n1.count_add >= n2.avg_add then 15
				when n1.count_sn !=0 and n1.count_click >= n2.avg_click and n1.count_detail >= n2.avg_detail and n1.count_add >= n2.avg_add then 16
				else 0 end
			) as tab,
			rand() as number
		from
		(
			select
				0 as tap,
				b1.user_id,
				(case when b2.count_click is null then 0 else b2.count_click end) as count_click,
				(case when b3.count_detail is null then 0 else b3.count_detail end) as count_detail,
				(case when b4.count_add is null then 0 else b4.count_add end) as count_add,
				(case when b5.count_sn is null then 0 else b5.count_sn end) as count_sn

			from
			(
				select
					distinct
					a1.user_id
				from
				(
					select
						distinct
						user_id
					from
						cc_ods_log_list_click_hourly
					where
						ds>= '${bizdate}'
						and
						ds<= '${bizdate1}'
				)a1
				left join
				(
					select
						distinct
						user_id
					from
						cc_ods_log_list_click_hourly
					where
						ds>= '${bizdate2}'
						and
						ds<= '${bizdate3}'
				)a2
				on a1.user_id=a2.user_id
				where a2.user_id is null
			)b1
			left join
			(
				select
					user_id,
					count(user_id) as count_click
				from
					cc_ods_log_list_click_hourly
				where
					ds>= '${bizdate}'
					and
					ds<= '${bizdate1}'
				group by
					user_id
			)b2
			on b1.user_id=b2.user_id
			left join
			(
				select
					user_id,
					count(distinct product_id) as count_detail
				from
					cc_ods_log_nginx_product_detail_hourly
				where
					ds>= '${bizdate}'
					and
					ds<= '${bizdate1}'
				group by
					user_id
			)b3
			on b1.user_id=b3.user_id
			left join
			(
				select
					user_id,
					count(distinct product_id) as count_add
				from
					cc_ods_log_nginx_cart_add_hourly
				where
					ds>= '${bizdate}'
					and
					ds<= '${bizdate1}'
				group by
					user_id
			)b4
			on b1.user_id=b4.user_id
			left join
			(
				select
					user_id,
					count(order_sn) as count_sn
				from
					cc_order_user_pay_time
				where
					ds>= '${bizdate}'
					and
					ds<= '${bizdate1}'
					and
					shop_id not in (148,9206,1378,17472) 
					and 
					deal_way not in(14,114,80,180,85,185,87,187)
					and
					source_channel !=2
				group by
					user_id
			)b5
			on b1.user_id=b5.user_id
		)n1
		left join 
		(
			select
				0 as tap,
				avg(p2.count_click) as avg_click,
				avg(p3.count_detail) as avg_detail,
				avg(p4.count_add) as avg_add
			from
			(
				select
					distinct
					t1.user_id
				from
				(
					select
						distinct
						user_id
					from
						cc_ods_log_list_click_hourly
					where
						ds>= '${bizdate}'
						and
						ds<= '${bizdate1}'
				)t1
				left join
				(
					select
						distinct
						user_id
					from
						cc_ods_log_list_click_hourly
					where
						ds>= '${bizdate2}'
						and
						ds<= '${bizdate3}'
				)t2
				on t1.user_id=t2.user_id
				where t2.user_id is null
			)p1
			left join
			(
				select
					user_id,
					count(user_id) as count_click
				from
					cc_ods_log_list_click_hourly
				where
					ds>= '${bizdate}'
					and
					ds<= '${bizdate1}'
				group by
					user_id
			)p2
			on p1.user_id=p2.user_id
			left join
			(
				select
					user_id,
					count(distinct product_id) as count_detail
				from
					cc_ods_log_nginx_product_detail_hourly
				where
					ds>= '${bizdate}'
					and
					ds<= '${bizdate1}'
				group by
					user_id
			)p3
			on p1.user_id=p3.user_id
			left join
			(
				select
					user_id,
					count(distinct product_id) as count_add
				from
					cc_ods_log_nginx_cart_add_hourly
				where
					ds>= '${bizdate}'
					and
					ds<= '${bizdate1}'
				group by
					user_id
			)p4
			on p1.user_id=p4.user_id
		)n2
	on n1.tap=n2.tap
	)n3
)n4
where n4.num <=1000
