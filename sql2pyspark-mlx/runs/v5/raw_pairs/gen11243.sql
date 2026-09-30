WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        SUBSTRING(product_name, 1, 4) AS product_name_prefix
    FROM products
    WHERE supplier_id IS NOT NULL
      AND category = 'Electronics'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        brand,
        category,
        price
    FROM valid
) src
PIVOT (
    SUM(price) AS value_sum,
    COUNT(1) AS item_count
    FOR category IN ('Grocery' AS grocery, 'Electronics' AS electronics, 'Home' AS home)
)
