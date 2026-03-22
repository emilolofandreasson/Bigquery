with source as (
    select * from {{ source('thelook_ecommerce', 'distribution_centers') }}
),

renamed as (
    select
        id as distribution_center_id,
        name as center_name,
        latitude,
        distribution_center_geom,
        longitude
    from source
)

select * from renamed