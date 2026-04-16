with source_data AS (
    SELECT * FROM {{ source('staging_raw_sources', 'sales_data') }}
)

SELECT *
FROM source_data