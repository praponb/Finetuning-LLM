WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        category,
        brand,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        LOWER(category) AS category_lower,
        ARRAY_CONTAINS(tags, 'premium') AS is_premium,
        COALESCE(brand, 'UNKNOWN') AS brand_clean
    FROM products
    WHERE launch_date >= DATE '2023-04-06'
      AND brand IS DISTINCT FROM 'Initech'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        supplier_id,
        category,
        stock_qty
    FROM filtered
) src
PIVOT (
    SUM(stock_qty)
    FOR category IN ('Home' AS home, 'Books' AS books)
);
