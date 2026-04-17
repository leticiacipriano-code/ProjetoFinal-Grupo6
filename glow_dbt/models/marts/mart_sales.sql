SELECT
    country,
    product,
    sale_year,
    price,
    boxes_shipped

FROM {{ ref('stg_sales_data') }}