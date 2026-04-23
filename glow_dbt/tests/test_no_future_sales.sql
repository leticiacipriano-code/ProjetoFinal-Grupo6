-- Singular Test: Validate that no sales have future dates
-- Purpose: Ensure data integrity by preventing sales records with dates beyond today
-- This test fails if any sales record has a date in the future

WITH future_sales AS (
  SELECT 
    country,
    product,
    date,
    price,
    boxes_shipped
  FROM {{ ref('mart_sales') }}
  WHERE date > CAST(CURRENT_DATE AS TEXT)
)

SELECT * FROM future_sales
