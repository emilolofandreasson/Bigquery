{{
    config(
        materialized='table',
        schema='Intermediate'
    )
}}

with source_events as (
    select * from {{ ref('stg_events') }}
    -- Vi inkluderar endast rader där user_id finns 
    where user_id is not null
),

ranked_events as (
    select
        *,
        -- Skapar ett radnummer per användare, nyast först 
        row_number() over (
            partition by user_id 
            order by event_at desc
        ) as event_rank
    from source_events
)

select
    -- Nycklar för framtida kopplingar 
    user_id as KEY_Customer,
    event_id as KEY_Event,    
    user_id,
    event_id,
    sequence_number,
    session_id,
    event_at,
    date(event_at) as event_date, -- För enklare datumhantering 
    ip_address,
    city,
    state,
    postal_code,
    browser,
    traffic_source,
    url_path,  -- Uppdaterat från uri 
    event_type
from ranked_events
-- Behåller endast den senaste raden per användare 
where event_rank = 1