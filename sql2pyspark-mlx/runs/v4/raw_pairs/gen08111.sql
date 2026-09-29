WITH base AS (
    SELECT *
    FROM order_items
    WHERE quantity < 3
),
quantity_bucket_summary AS (
    SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(DISTINCT unit_price) AS unique_orders
    FROM base
    GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM quantity_bucket_summary
WHERE unique_orders >= 5
ORDER BY quantity_bucket, unique_orders DESC;
