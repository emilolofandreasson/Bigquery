{{
    config(
        materialized='table',
        schema='Marts'
    )
}}

with customers as (
    select * from {{ ref('int_users') }}
),

latest_events as (
    select * from {{ ref('int_events_latest_per_user') }}
),

final as (
    select
        -- Nyckel för koppling i Qlik
        c.KEY_Customer,
        
        -- Kundinformation
        c.full_name,
        c.age,
        c.age_group,
        c.Gender,
        c.city,
        c.state,
        c.country,
        c.signup_date,

        -- Event-information (Senaste källan och aktiviteten)
        e.traffic_source as latest_traffic_source,
        e.event_type as latest_event_type,
        e.url_path as latest_url_path,
        e.event_at as latest_event_at

    from customers c
    left join latest_events e 
        on c.KEY_Customer = e.KEY_Customer
)

select * from final