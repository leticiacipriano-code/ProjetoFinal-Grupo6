with joined AS (
    SELECT * FROM {{ ref('int_skincare_enrichment') }}
),

mapping AS (
    SELECT * 
    FROM {{ref('skincare_label_mapping')}}
)

SELECT
    j.*,
    coalesce(lower(map.standard), lower(j.product_type)) AS label_mapped
FROM
    joined AS j
LEFT JOIN
    mapping AS map
ON
    lower(j.product_type) = lower(map.raw)
