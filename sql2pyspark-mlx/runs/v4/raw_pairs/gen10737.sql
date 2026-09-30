WITH valid AS (
    -- Basic cleansing and defaults
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
        DATE_TRUNC('MONTH', launch_date) AS launch_date_period,
        LENGTH(category) AS category_length,
        COALESCE(brand, 'UNKNOWN') AS resolved_brand
    FROM products
    WHERE launch_date >= TO_DATE('2023-12-07')
      AND supplier_id IS NOT NULL
      AND supplier_id IS NULL
)
-- Aggregate with conditional metrics
SELECT
    brand,
    MAX(launch_date_period) AS last_seen,
    SIZE(COLLECT_SET(category)) AS num_categories,
    SUM(CASE WHEN category = 'Grocery' THEN price ELSE 0 END) AS grocery_price
FROM valid
GROUP BY brand
