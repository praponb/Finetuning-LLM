WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        DATEDIFF(CURRENT_DATE(), launch_date) days_since_launch_date,
        COALESCE(brand, 'UNKNOWN') brand_clean
    FROM products
    WHERE supplier_id IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        brand,
        cost,
        price
    FROM filtered
) src
UNPIVOT (
    reading FOR metric_name IN (cost AS COST_VAL, price)
)
