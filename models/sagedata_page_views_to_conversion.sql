{{
  config(
    tags='sagedata_attribution'
  )
}}


with pv as (
    SELECT pv.*
         , {{ channel_classification() }} as channel
         , c.conversion_tstamp
    FROM {{source('derived', 'snowplow_web_page_views')}} pv
        JOIN {{source('derived', 'snowplow_fractribution_conversions_by_customer_id')}} c
    ON ('f'+pv.domain_userid) = c.customer_id
        AND pv.derived_tstamp > cast({{ dbt.dateadd('day', (- var('snowplow__path_lookback_days') + 1), 'conversion_tstamp') }} as date)
        AND pv.derived_tstamp <= c.conversion_tstamp
)
   ,classified_pv as (
    -- Only gets the pageviews within the conversion window prior to the conversion event
    SELECT *
         , row_number() over (partition by domain_userid, conversion_tstamp order by derived_tstamp, dvce_created_tstamp) AS page_view_pre_conv_index_first_touch
         , row_number() over (partition by domain_userid, conversion_tstamp order by derived_tstamp DESC, dvce_created_tstamp DESC) AS page_view_pre_conv_index_last_touch

         , CASE WHEN channel = 'Unmatched_Channel' THEN TRUE ELSE FALSE END channel_is_unmatched
         , CASE WHEN channel not in ('Unmatched_Channel','Direct', 'Organic', 'Search', 'Internal') THEN TRUE ELSE FALSE END channel_is_marketing
    FROM pv
)
   , ordered_pv as (
    -- Only gets the pageviews within the conversion window prior to the conversion event
    SELECT *
         , CASE WHEN channel_is_unmatched = FALSE THEN row_number() over (partition by domain_userid, conversion_tstamp, channel_is_unmatched order by derived_tstamp, dvce_created_tstamp) ELSE NULL END AS page_view_pre_conv_chanel_index_asc
         , CASE WHEN channel_is_unmatched = FALSE THEN row_number() over (partition by domain_userid, conversion_tstamp, channel_is_unmatched order by derived_tstamp DESC, dvce_created_tstamp DESC) ELSE NULL END AS page_view_pre_conv_chanel_index_desc

         , CASE WHEN channel_is_marketing = TRUE THEN row_number() over (partition by domain_userid, conversion_tstamp, channel_is_marketing order by derived_tstamp, dvce_created_tstamp) ELSE NULL END AS page_view_pre_conv_mkt_chanel_index_asc
         , CASE WHEN channel_is_marketing = TRUE THEN row_number() over (partition by domain_userid, conversion_tstamp, channel_is_marketing order by derived_tstamp DESC, dvce_created_tstamp DESC) ELSE NULL END AS page_view_pre_conv_mkt_chanel_index_desc

         -- Here we assume that we will attribute per day per mkt_column that is defined in project.xml. This means we need to count how many event per day happened for each partition of the mkt_column.
         ,CASE WHEN {{var('sagedata_attribution_mkt_column')}} is not NULL THEN COUNT(1) over (partition by {{var('sagedata_attribution_mkt_column')}}, derived_tstamp::date) ELSE NULL END AS count_events_per_day_per_att_mkt_column
    FROM classified_pv
)
   , count_of_mkt_events AS (
    SELECT *
         ,COUNT (page_view_pre_conv_chanel_index_asc) OVER (partition by domain_userid, conversion_tstamp) count_of_touchpoints_per_conversion
         ,COUNT (page_view_pre_conv_mkt_chanel_index_asc) OVER (partition by domain_userid, conversion_tstamp) count_of_mkt_touchpoints_per_conversion
    FROM ordered_pv
)
-- COST is optional, if we want to attribute costs, but that should follow attribution model prefered by the client. Bellow is the example of liner model.
SELECT cm.*
     ,CASE WHEN count_events_per_day_per_att_mkt_column > 0 THEN cost.spend / count_events_per_day_per_att_mkt_column ELSE NULL END cost_per_event_linear
FROM count_of_mkt_events cm
         LEFT JOIN {{ref('sagedata_channel_spend_day')}} cost ON cm.derived_tstamp::date = cost.reporting_dt AND cm.{{var('sagedata_attribution_mkt_column')}} = cost.{{var('sagedata_attribution_mkt_column')}}