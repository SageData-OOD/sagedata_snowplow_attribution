/* User supplied SQL script to clean the campaign name
   Example TRIM({{ column_name }})
*/
{% macro clean_campaign_name(column_name) %}
    {{ column_name }}
{% endmacro %}