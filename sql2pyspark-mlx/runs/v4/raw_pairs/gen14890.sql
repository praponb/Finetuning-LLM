WITH base AS (
    SELECT *
    FROM products
    WHERE supplier_id IN (19, 28) AND product_name IS NOT NULL AND category IN ('Home', 'Fashion', 'Sports')
),
products_stats AS (
    SELECT category, MAX(launch_date) AS latest_launch_date, COUNT(DISTINCT cost) AS unique_brands
    FROM base
    GROUP BY category
)
SELECT category, latest_launch_date
FROM products_stats
WHERE unique_brands > 10
ORDER BY category ASC, latest_launch_date DESC
LIMIT 5;
