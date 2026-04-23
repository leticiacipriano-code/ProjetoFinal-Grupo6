SELECT
    sales_person, -- Adicione esta linha
    country,
    product,
    date,
    price,
    boxes_shipped
FROM {{ ref('stg_sales_data') }}