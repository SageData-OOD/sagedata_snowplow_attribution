{{
  config(
    materialized='view',
    tags='sagedata_attribution'
  )
}}
-- Create view with Channel Classification
SELECT *
, {{ channel_classification() }} as channel
FROM {{source('derived','snowplow_web_sessions')}}