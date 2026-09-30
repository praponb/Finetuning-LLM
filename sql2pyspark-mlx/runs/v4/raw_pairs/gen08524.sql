WITH payments_by_order AS (
    SELECT order_id, MAX(amount) - MIN(amount) AS amount_range
    FROM payments
    GROUP BY order_id
)
SELECT o.id, o.order_date, pbo.amount_range
FROM orders o
LEFT JOIN payments_by_order pbo ON o.id = pbo.order_id
WHERE pbo.amount_range > 25
ORDER BY id;
