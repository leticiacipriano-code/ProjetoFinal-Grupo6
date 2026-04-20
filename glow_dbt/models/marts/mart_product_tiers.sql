{{ config(materialized='table') }}

-- mart_product_tiers
-- Atribui a categoria de negócio baseada no quartil (usando mart_price_quartiles)

with price_quartiles as (
    select * from {{ ref('mart_price_quartiles') }}
),

product_tiers as (
    SELECT 
        product_name,
        ingredients_list,
        rank_num as product_rank,
        price,
        CASE 
            WHEN quartile = 4 THEN 'Premium'
            WHEN quartile IN (2, 3) THEN 'Mid-Ticket'
            ELSE 'Budget'
        END as price_tier
    FROM price_quartiles
)

select *
from product_tiers
