{{ config(materialized='table') }}

-- mart_pair_stats
-- Calcula a performance (volume e rank médio) de cada par de ingredientes por faixa de preço (price_tier).

with ingredient_pairs as (
    select * from {{ ref('mart_ingredient_pairs') }}
)

select
    ing_1,
    ing_2,
    price_tier,
    count(distinct product_name) as volume_produtos,
    avg(product_rank) as avg_rank
from ingredient_pairs
group by 1, 2, 3

-- Resultado: ing_1, ing_2, price_tier, volume_produtos, avg_rank
