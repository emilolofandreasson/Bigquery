{{
    config(
        materialized='table',
        schema='Intermediate'
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
),

dist_centers as (
    select * from {{ ref('distribution_centers') }}
),

joined as (
    select
        p.product_id          as KEY_Product,
        round(p.cost)         as UnitCost,
        round(p.retail_price) as UnitPrice,
        p.product_name        as product_name,
        p.brand,
        p.category,
        p.department,
        p.sku,
        -- Information från Distribution Centers
        dc.center_name as dist_center_name
       -- dc.latitude as dist_center_lat,
        --dc.longitude as dist_center_long
    from products p
    left join dist_centers dc 
        on p.distribution_center_id = dc.distribution_center_id
)

select * from joined