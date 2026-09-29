WITH valid AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        supplier_id,
        COALESCE(stock_qty, 0) AS stock_qty_filled,
        COALESCE(brand, 'DIRECT') AS brand_filled
    FROM products
    WHERE brand IS NOT NULL
)
SELECT
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    SUM(CASE WHEN category = 'Sports' THEN stock_qty_filled ELSE 0 END) AS sports_stock_qty_filled,
    COUNT(*) AS event_count,
    COUNT(*) AS cnt
FROM valid
GROUP BY activity_month
HAVING SUM(CASE WHEN category = 'Sports' THEN stock_qty_filled ELSE 0 END) >= 0;
