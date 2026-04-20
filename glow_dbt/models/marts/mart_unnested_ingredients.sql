{{ config(materialized='table') }}

-- mart_unnested_ingredients
-- Gera uma linha por ingrediente a partir de `mart_price_quartiles` (evita dependência
-- de versões diferentes de `mart_product_tiers`).

with price_quartiles as (
    select * from {{ ref('mart_price_quartiles') }}
),

product_tiers as (
    select
        pq.product_name,
        pq.quartile,
        pq.ingredients_list,
        pq.rank_num as product_rank,
        case
            when pq.quartile = 4 then 'Premium'
            when pq.quartile in (2,3) then 'Mid-Ticket'
            else 'Budget'
        end as price_tier
    from price_quartiles pq
)

select
    t.product_name,
    t.product_rank,
    t.price_tier,
    trim(both ' ' from u.ingredient) as ingredient
from product_tiers t
cross join lateral (
    select unnest(string_to_array(coalesce(t.ingredients_list, ''), ','))::text as ingredient
) u
where trim(both ' ' from coalesce(u.ingredient, '')) <> ''
