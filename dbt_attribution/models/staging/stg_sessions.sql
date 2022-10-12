{{
    config(
        materialized='incremental',
        unique_key='session_id'
    )
}}

select * from {{source('attribution', 'raw_sessions')}}


{% if is_incremental() %}

  where session_id >= (select max(session_id) from {{ this }})

{% endif %}
