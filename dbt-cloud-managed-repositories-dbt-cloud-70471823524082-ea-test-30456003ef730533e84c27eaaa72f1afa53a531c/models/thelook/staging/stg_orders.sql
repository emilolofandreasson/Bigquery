with source as (
    -- dbt hämtar definitionen från din src_thelook.yml
    select * from {{ source('thelook_ecommerce', 'orders') }}
),

renamed as (
    select
        order_id,
        user_id,
        status as order_status,
        gender as customer_gender,
        created_at as order_created_at,
        returned_at,
        shipped_at,
        delivered_at,
        num_of_item as num_items_ordered
    from source
)

select * from renamed