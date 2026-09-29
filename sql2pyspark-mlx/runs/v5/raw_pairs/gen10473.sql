WITH recent_orders AS (
    SELECT *
    FROM orders
    WHERE store_id IS NOT NULL
),
status_summary AS (
    SELECT status, MAX(discount) - MIN(discount) AS discount_range
    FROM recent_orders
    GROUP BY status
)
SELECT status, discount_range
FROM status_summary
WHERE discount_range < 63
ORDER BY status
LIMIT 50;
