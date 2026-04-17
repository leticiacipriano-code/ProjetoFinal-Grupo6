with source_data AS (
    SELECT * FROM {{ source('staging_raw_sources', 'sales_data') }}
)

SELECT
    sales_person,
    country,
    product,
    date,
    EXTRACT(year FROM date::date) AS sale_year,
    amount AS price,
    boxes_shipped

FROM source_data