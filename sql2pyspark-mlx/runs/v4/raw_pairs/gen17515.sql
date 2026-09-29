WITH ValidOrders AS (
    SELECT *
    FROM orders
    WHERE store_id IN (1, 15, 21, 33) AND (customer_id IN (9, 22, 32, 33) OR shipping_fee <= 19)
),
agg AS (
    SELECT channel, COUNT(discount) AS discount_count
    FROM ValidOrders
    GROUP BY channel
)
SELECT *
FROM agg
WHERE discount_count < 8
ORDER BY channel
LIMIT 3;
