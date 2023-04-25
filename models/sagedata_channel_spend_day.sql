{{
  config(
    tags='sagedata_attribution'
  )
}}

-- By default, the model assigns an example 10k spend to each channel found in channel_counts
-- TODO: put in your own spend by day calculations per channel in the channel_spend macro in your own dbt project


{{ channel_spend_day() }}