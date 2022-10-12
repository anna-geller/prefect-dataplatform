{{
    config(
        materialized='incremental',
        unique_key='date_day'
    )
}}

select * from {{source('attribution', 'raw_ad_spend')}}


{% if is_incremental() %}

  where date_day >= (select max(date_day) from {{ this }})

{% endif %}
