{{
  config(
    tags='sagedata_attribution'
  )
}}


with first_touch as (
    SELECT conversion_tstamp,
           derived_tstamp,
           collector_tstamp,
           domain_sessionid,
           domain_userid,
           channel,
           mkt_medium,
           mkt_source,
           mkt_term,
           mkt_content,
           mkt_campaign,
           mkt_clickid,
           mkt_network,
           count_of_mkt_touchpoints_per_conversion
    FROM {{ref('sagedata_page_views_to_conversion')}}
    WHERE page_view_pre_conv_index_first_touch = 1 -- first touch regardless of what it was
)
   ,first_touch_mkt_channel as (
    SELECT conversion_tstamp,
           derived_tstamp,
           collector_tstamp,
           domain_sessionid,
           domain_userid,
           channel,
           mkt_medium,
           mkt_source,
           mkt_term,
           mkt_content,
           mkt_campaign,
           mkt_clickid,
           mkt_network
    FROM {{ref('sagedata_page_views_to_conversion')}}
    WHERE page_view_pre_conv_mkt_chanel_index_asc = 1 -- first touch regardless of what it was
)
   ,last_touch_mkt_channel as (
    SELECT conversion_tstamp,
           derived_tstamp,
           collector_tstamp,
           domain_sessionid,
           domain_userid,
           channel,
           mkt_medium,
           mkt_source,
           mkt_term,
           mkt_content,
           mkt_campaign,
           mkt_clickid,
           mkt_network
    FROM {{ref('sagedata_page_views_to_conversion')}}
    WHERE page_view_pre_conv_mkt_chanel_index_desc = 1 -- first touch regardless of what it was
)

SELECT f.*
     ,ftm.channel first_touch_mkt_channel
     ,ftm.mkt_medium first_touch_mkt_medium
     ,ftm.mkt_source first_touch_mkt_source
     ,ftm.mkt_term first_touch_mkt_term
     ,ftm.mkt_content first_touch_mkt_content
     ,ftm.mkt_campaign first_touch_mkt_campaign
     ,ftm.mkt_network first_touch_mkt_network

     ,ltm.channel last_touch_mkt_channel
     ,ltm.mkt_medium last_touch_mkt_medium
     ,ltm.mkt_source last_touch_mkt_source
     ,ltm.mkt_term last_touch_mkt_term
     ,ltm.mkt_content last_touch_mkt_content
     ,ltm.mkt_campaign last_touch_mkt_campaign
     ,ltm.mkt_network last_touch_mkt_network
FROM first_touch f
         LEFT JOIN first_touch_mkt_channel ftm ON f.conversion_tstamp = ftm.conversion_tstamp AND f.domain_userid = ftm.domain_userid
         LEFT JOIN last_touch_mkt_channel ltm ON f.conversion_tstamp = ltm.conversion_tstamp AND f.domain_userid = ltm.domain_userid

