{{ config(materialized='view') }}

-- mart_ingredient_pairs
-- Gera pares únicos de ingredientes por produto a partir de `mart_unnested_ingredients`.

with unnested as (
    select * from {{ ref('mart_unnested_ingredients') }}
)

select distinct
    u.product_name,
    u.product_rank,
    u.price_tier,
    case when u.ingredient < v.ingredient then u.ingredient else v.ingredient end as ing_1,
    case when u.ingredient < v.ingredient then v.ingredient else u.ingredient end as ing_2
from unnested u
join unnested v
    on u.product_name = v.product_name
    and u.ingredient < v.ingredient

