WITH src AS (
    SELECT *
    FROM order_items
    WHERE product_id IS NOT NULL
),
order_items_stats AS (
    SELECT CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, AVG(quantity) AS avg_quantity, COUNT(*) AS total_count
    FROM src
    GROUP BY CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM order_items_stats
WHERE total_count < 7
ORDER BY quantity_bucket DESC
