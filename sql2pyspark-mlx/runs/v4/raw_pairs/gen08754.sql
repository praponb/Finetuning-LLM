WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(brand, '/', product_name) AS brand_product_name
    FROM products
    WHERE supplier_id IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        supplier_id,
        category,
        price
    FROM base
) src
PIVOT (
    SUM(price)
    FOR category IN ('Books' AS books, 'Sports' AS sports, 'Home' AS home)
)
