{{
    config(
        materialized='table',
        schema='Intermediate'
    )
}}

with users as (
    select * from {{ ref('stg_users') }}
),

transformed as (
    select
        -- Använd backticks runt namnet för att tillåta %
        user_id as KEY_Customer, 
        user_id,
        concat(first_name, ' ', last_name) as full_name,
        age,
        case 
            when age < 18 then 'Under 18'
            when age between 18 and 35 then '18-35'
            when age between 36 and 50 then '36-50'
            else '50+'
        end as age_group,

        CASE 
            WHEN gender = 'M' then 'Men' -- Prova gärna = istället för LIKE om det är exakta värden
            WHEN gender = 'F' then 'Female'
            else 'Unknown'
        end as Gender,
        
        city,
        state,
        country,
        traffic_source,
        date(user_created_at) as signup_date
    from users
)

select * from transformed