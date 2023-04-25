{{
  config(
    schema = 'derived',
    materialized='view',
    tags='sagedata_attribution'
  )
}}

SELECT *
FROM {{ref('snowplow_web_page_views_w_channel')}}
WHERE mkt_source IS NOT NULL