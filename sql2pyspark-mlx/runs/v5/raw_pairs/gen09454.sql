WITH base AS (
    SELECT *
    FROM order_items
    WHERE unit_price BETWEEN 170 AND 240
),
order_items_stats AS (
    SELECT CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, order_id, SUM(quantity * unit_price) AS revenue
    FROM base
    GROUP BY CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END, order_id
)
SELECT quantity_bucket, order_id, revenue
FROM order_items_stats
WHERE revenue >= 76
ORDER BY quantity_bucket ASC;
