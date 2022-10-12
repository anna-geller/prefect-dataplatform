{{
	config(materialized='view')
}}

with sessions as (

    select * from {{ ref('stg_sessions') }}

),

customer_conversions as (

    select * from {{ ref('stg_customer_conversions') }}

),

sessions_before_conversion as (

    select
        *,

        count(*) over (
            partition by customer_id
        ) as total_sessions,

        row_number() over (
            partition by customer_id
            order by sessions.started_at
        ) as session_index

    from sessions

    left join customer_conversions using (customer_id)

    where sessions.started_at <= customer_conversions.converted_at
        and sessions.started_at >= dateadd(days, -30, customer_conversions.converted_at)

),

with_points as (

    select
        *,

        case
            when session_index = 1 then 1.0
            else 0.0
        end as first_touch_points,

        case
            when session_index = total_sessions then 1.0
            else 0.0
        end as last_touch_points,

        case
            when total_sessions = 1 then 1.0
            when total_sessions = 2 then 0.5
            when session_index = 1 then 0.4
            when session_index = total_sessions then 0.4
            else 0.2 / (total_sessions - 2)
        end as forty_twenty_forty_points,

        1.0 / total_sessions as linear_points,

        revenue * first_touch_points as first_touch_revenue,
        revenue * last_touch_points as last_touch_revenue,
        revenue * forty_twenty_forty_points as forty_twenty_forty_revenue,
        revenue * linear_points as linear_revenue

    from sessions_before_conversion

)

select * from with_points
