
{{
    config(
        materialized='table',
        schema='Marts'
    )
}}


with tmpTable as (
SELECT * FROM 
{{ref('int_products')}}

)
,
GP as  (
SELECT 
KEY_Product,
UnitPrice - UnitCost as GrossProfit
FROM tmpTable
)
,
tmpFinal as (
SELECT 
M.KEY_Product,
M.UnitPrice,
G.GrossProfit,
M.UnitCost,
M.product_name,
--M.brand,
M.category,
--M.department,
--M.sku
From tmpTable M
Left Join GP G
on M.KEY_Product = G.KEY_Product
)
,
category_avg as (
    SELECT
        category,
        AVG(GrossProfit) as Avg_Category_GrossProfit
    FROM tmpFinal 
    GROUP BY category
)
,
Final as (
SELECT 
--f.KEY_Product,
--f.UnitPrice,
--f.GrossProfit,
--f.UnitCost,
--f.product_name,
--M.brand,
f.category as Category,
round(c.Avg_Category_GrossProfit,2) as Avg_Category_GP
FROM tmpFinal f
Left Join category_avg c on c.category = f.category
)

select Distinct 
Category,
Avg_Category_GP
From Final





