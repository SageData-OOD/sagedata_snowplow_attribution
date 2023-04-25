/* User supplied SQL script to extract total ad spend by day by campaign, by replacing the SQL query in the default__channel_spend_day() macro.
   Required output schema:
   reporting_dt: DATE Not NULL
   mkt_campaign_clean: STRING not NULL
   spend: FLOAT64 (Use the same monetary units as conversion revenue, and NULL if unknown.)

  Example (simplified) query:

  SELECT segments__date reporting_dt
     , {{ clean_campaign_name('campaign__name') }} mkt_campaign
     , campaign__name  mkt_campaign_original
     ,'google' mkt_source
     , SUM(metrics__cost_micros/1000000::float) spend
    FROM {{ source('STAGE','GOOLGE_ADS_TABLE') }}
    GROUP BY 1,2,3

  Example table output for the user-supplied SQL:

  reporting_dt  |mkt_campaign_clean  |  Spend
 ------------------------
  2023-01-01             |direct                |  1050.02
  2023-01-01             |paid_search           |  10490.11
  2023-01-01             |etc... */

{% macro channel_spend_day() %}
  {{ return(adapter.dispatch('channel_spend_day', 'sagedata_snowplow_attribution')()) }}
{% endmacro %}

{% macro default__channel_spend_day() %}

SELECT segments__date reporting_dt
     , {{ clean_campaign_name('campaign__name') }} mkt_campaign
     , campaign__name  mkt_campaign_original
     ,'google' mkt_source
     , SUM(metrics__cost_micros/1000000::float) spend
FROM {{ source('STAGE','GOOLGE_ADS_TABLE') }}
GROUP BY 1,2,3

{% endmacro %}
