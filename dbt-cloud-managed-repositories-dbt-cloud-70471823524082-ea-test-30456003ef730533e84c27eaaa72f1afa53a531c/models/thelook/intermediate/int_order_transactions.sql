{{
    config(
        materialized='incremental',
        unique_key='KEY_OrderItem', 
        schema='Intermediate'
    )
}}

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select 
        order_id,
        user_id,
        order_status,
        order_created_at as order_date,
        num_items_ordered
    from {{ ref('stg_orders') }}
),

joined as (
    select
        -- Nycklar
        oi.order_item_id as KEY_OrderItem,
        oi.order_id as KEY_Order,
        oi.user_id as KEY_Customer,
        oi.product_id as KEY_Product,

        -- Dimensionell info
        Date(o.order_date)          as OrderDate, -- Alias med stort O
        o.order_status              as order_status,
        o.num_items_ordered         as total_items_in_order,
        oi.item_status              as Item_status,
        
        round(oi.sale_price)        as sale_price,
        Date(oi.item_shipped_at)    as ShippedDate,
        Date(oi.item_delivered_at)  as DeliveredDate,
        Date(oi.item_returned_at)   as ReturnedDate,

        case when o.order_status != 'Returned' then oi.sale_price else 0 end as net_sales_amount

    from order_items oi
    inner join orders o 
        on oi.order_id = o.order_id

 {% if is_incremental() %}
      -- Jämför källans rådata (o.order_date) med 
      -- mål-tabellens alias (OrderDate)
      where unix_date(date(o.order_date)) > unix_date((select max(OrderDate) from {{ this }}))
    {% endif %}
)

select * from joined


