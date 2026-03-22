{{
    config(
        materialized='table',
        schema='Marts'
    )
}}

with Source as (
    select * from {{ ref('int_order_transactions') }}
),

FACT_orders as (

            select 
            *
            From source
)

select * From FACT_orders