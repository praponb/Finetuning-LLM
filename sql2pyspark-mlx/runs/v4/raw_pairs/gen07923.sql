WITH RecentOrders AS (
    SELECT *
    FROM orders
    WHERE customer_id = 12
),
orders_stats AS (
    SELECT status, ROUND(AVG(amount), 2) AS avg_amount
    FROM RecentOrders
    GROUP BY status
)
SELECT *
FROM orders_stats
ORDER BY status
