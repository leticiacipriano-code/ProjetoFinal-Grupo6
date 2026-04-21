-- Singular Test: Validate that all products have valid price quartiles assigned
-- Purpose: Ensure no products are missing quartile assignments (data completeness)
-- This test fails if any product has NULL or invalid quartile value

WITH invalid_quartiles AS (
  SELECT 
    product_name,
    price,
    quartile,
    CASE 
      WHEN quartile IS NULL THEN 'NULL_QUARTILE'
      WHEN quartile NOT IN (1, 2, 3, 4) THEN 'INVALID_QUARTILE_VALUE'
    END AS error_type
  FROM {{ ref('mart_price_quartiles') }}
  WHERE quartile IS NULL OR quartile NOT IN (1, 2, 3, 4)
)

SELECT * FROM invalid_quartiles
