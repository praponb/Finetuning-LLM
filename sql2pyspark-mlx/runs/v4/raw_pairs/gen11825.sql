WITH base AS (
    SELECT *
    FROM orders
    WHERE shipping_fee BETWEEN 18 AND 38
),
orders_stats AS (
    SELECT status, MAX(discount) - MIN(discount) AS discount_range, MAX(order_date) AS last_order_date
    FROM base
    GROUP BY status
)
SELECT *
FROM orders_stats;
