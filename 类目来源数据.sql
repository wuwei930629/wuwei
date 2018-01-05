 select
  p1.ds,
  p2.name,
  p1.source,
  sum(p1.ipv) as ipv,
  sum(p1.ipv_uv) as ipv_uv,
  sum(p1.gmv) as gmv,
  sum(p1.count_sn) as count_sn,
  sum(p1.count_user) as count_user,
  sum(p1.fee) as fee
 from
  (
    select
    t1.ds,
    t1.shop_id,
    t1.source,
    t2.ipv,
    t2.ipv_uv,
    t1.gmv,
    t1.count_sn,
    t1.count_user,
    t1.fee
  from
  (
      select
        a1.ds,
        a1.shop_id,
        a1.source,
        count(a1.order_sn) as count_sn,
        sum(a1.pay_fee) as fee,
        count(distinct a1.user_id) as count_user,
        sum(a1.total_fee) as gmv
      from
      (
        select
        ds,
        shop_id,
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
        when deal_way in (11,111) then '收藏'
        when deal_way in (5,105) then '购物车'
          when deal_way in (15092036,15092136,15091936) then '良品'
          when deal_way in (90,91,92,93,94,95,96,99,190,191,192,193,194,195,196,199) then '推荐'
          when deal_way in (15287069,15299064,15322042,15287169,15299164,15322142) then '品牌折扣'
        when deal_way >9000000 and deal_way <30000000 then '活动页'
        when deal_way >80000000 and deal_way <90000000 then '潮青年'
        else '其他' end) as source,
        order_sn,
        pay_fee,
        user_id,
        total_fee
      from
        origin_common.cc_order_user_pay_time 
      where
        ds>=20171207
        and
        shop_id not in(1378,148,9206)
        and
        source_channel !=2
        and
        deal_way not in(14,114,80,180,85,185,87,187)
      )a1
      group by
        a1.ds,
        a1.shop_id,
        a1.source
  )t1
  left join
  (
      select
        a2.ds,
        a2.shop_id,
        a2.source,
        sum(a2.pv) as ipv,
        sum(a2.uv) as ipv_uv
      from
      (
         select
         ds,
        shop_id,
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
          when track_id in (11,111) then '收藏'
          when track_id in (5,105) then '购物车'
            when track_id in (15092036,15092136,15091936) then '良品'
            when track_id in (90,91,92,93,94,95,96,99,190,191,192,193,194,195,196,199) then '推荐'
            when track_id in (15287069,15299064,15322042,15287169,15299164,15322142) then '品牌折扣'
          when track_id >9000000 and track_id <30000000 then '活动页'
          when track_id >80000000 and track_id <90000000 then '潮青年'
          else '其他' end) as source,
          uv,
          pv
        from
          data.cc_dm_count_shop_product_source
        where
          ds>=20171207
          and
          track_id not in(14,114,80,180,85,185,87,187)
      )a2
      group by
        a2.ds,
        a2.shop_id,
        a2.source
  )t2
  on t1.ds=t2.ds and t1.shop_id=t2.shop_id and t1.source=t2.source
  )p1
left join
(
    select
      s1.shop_id,
      s2.name
    from
      cc_ods_fs_business_basic s1
    join
      cc_ods_fs_category s2
    on s1.category1 = s2.cid
)p2
on p1.shop_id=p2.shop_id
group by
  p1.ds,
  p2.name,
  p1.source