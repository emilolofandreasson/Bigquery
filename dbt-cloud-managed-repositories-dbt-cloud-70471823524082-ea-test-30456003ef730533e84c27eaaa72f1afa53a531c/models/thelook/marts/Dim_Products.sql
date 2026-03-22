{{
    config(
        materialized='table',
        schema='Marts'
    )
}}


with products as (
    select * from {{ ref('int_products') }}
),

Dim_Products as (

            select 
            *
            From products
)

select * From Dim_Products