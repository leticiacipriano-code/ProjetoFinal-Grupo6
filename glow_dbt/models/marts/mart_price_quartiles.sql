{{ config(materialized='table') }}

-- mart_price_quartiles
-- Divide os produtos em 4 faixas de preço (1 = mais barato, 4 = mais caro)
-- baseado em `mart_unified_products` do seu projeto dbt.

with source_data as (
    select
        product_name,
        brand,
        ingredients_list,
        -- limpa e converte price para numeric (remove símbolos/virgulas potenciais)
        case
            when trim(regexp_replace(coalesce(price::text, ''), '[^0-9.]', '', 'g')) = '' then null
            else trim(regexp_replace(coalesce(price::text, ''), '[^0-9.]', '', 'g'))::numeric
        end as price_num,
        -- limpa e converte rank para numeric
        case
            when trim(regexp_replace(coalesce(rank::text, ''), '[^0-9.]', '', 'g')) = '' then null
            else trim(regexp_replace(coalesce(rank::text, ''), '[^0-9.]', '', 'g'))::numeric
        end as rank_num
    from {{ ref('mart_unified_products') }}
),

price_quartiles as (
    select
        product_name,
        ingredients_list,
        rank_num,
        price_num as price,
        ntile(4) over (order by price_num) as quartile
    from source_data
)

select *
from price_quartiles
