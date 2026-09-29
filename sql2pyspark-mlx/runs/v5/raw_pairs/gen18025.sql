SELECT
    id,
    amount,
    customer_id,
    ROUND(discount * 100, 2) AS discount_scaled
FROM orders
WHERE customer_id IN (24, 25) AND customer_id IN (33, 35, 37, 40)
ORDER BY id
LIMIT 5;
