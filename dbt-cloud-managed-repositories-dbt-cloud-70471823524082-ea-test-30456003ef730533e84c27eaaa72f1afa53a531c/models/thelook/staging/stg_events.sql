with source as (
    select * from {{ source('thelook_ecommerce', 'events') }}
),

renamed as (
    select
        id as event_id,
        user_id,
        sequence_number,
        session_id,
        created_at as event_at,
        ip_address,
        city,
        state,
        postal_code,
        browser,
        traffic_source,
        uri as url_path,
        event_type
    from source
)

select * from renamed