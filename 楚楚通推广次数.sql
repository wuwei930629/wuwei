select
      h3.app_item_id,
      count(distinct h1.cck_uid),
      count(h1.ad_id)
from
(
    select
      ad_id
    from
      cc_ods_dwxk_wk_sales_deal_ctime
    where
      ds >=20171205
      and
      ds <=20171207 
)h4
left join
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
    zone in ('headsharecctafp','footersharecctafp','ccthellproductpromotion','cctseckillproductpromotion','list_share_massproduct','list_product_code',
             'list_share_79','list_share_912','list_share_1215','list_share_1519','list_share_1922','list_share_22','btn_share','list_share','list_share_product','prompt_share_itemlist',
             'search_prompt_share_itemlist','list_share_search_moments','list_share_search_group','list_share_item','list_share_search','list_massproduct','list_share_story',
             'list_share_story_group','list_share_story_moments')
  union all
    select
        cck_uid,
        ad_id
    from cc_ods_dwxk_cck_items_create_time
    where 
      ds >=20171205
      and
      ds <=20171207
)h1
on h4.ad_id=h1.ad_id
left join
(
    select
      ad_id,
      ad_item,
      ad_name
    from
      cc_ods_dwxk_fs_wk_ad_items
)h2
on h1.ad_id=h2.ad_id
left join
(
    select
      item_id,
      app_item_id
    from
      cc_ods_dwxk_fs_wk_items
)h3
on h2.item_id=h3.item_id
group by
  h3.app_item_id

