 select
    count(distinct f1.ad_id) as fx_prd_cnt,
    count(distinct f1.user_id) as cck_user_cnt
  from
  (
    select
      user_id as cck_uid,
      ad_id
    from 
    	cc_ods_log_gwapp_click_hourly
    where 
    	ds >=20171205
    	and
    	ds <=20171207
    	and 
    	(ad_id != '' or ad_id IS NOT NULL)
    union all
    select
      cck_uid,
      ad_id
    from 
    	cc_ods_dwxk_cck_items_create_time
    where 
    	ds >=20171205
    	and
    	ds <=20171207
    union all
    select
      s1.user_id as cck_uid,
      s2.ad_id   as ad_id
    from
    (
      select
      	distinct
        ad_type,
        ad_material_id,
        user_id
      from 
      	cc_ods_log_gwapp_click_hourly
      where 
      	ds >=20171205
    	and
    	ds <=20171207
      	and 
      	(ad_id != '' or ad_id IS NOT NULL)
    ) s1
    inner join
    (
      select
        distinct 
        ad_type,
        ad_material_id,
        ad_id
      from 
      	data.cc_dm_gwapp_new_ad_material_relation_hourly
      where 
      	ds >=20171205
    	and
    	ds <=20171207
    ) s2
    on s1.ad_type=s2.ad_type and s1.ad_material_id=s2.ad_material_id
  )f1