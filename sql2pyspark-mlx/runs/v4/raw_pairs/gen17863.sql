SELECT
    o.id,
    o.shipping_fee,
    o.amount,
    t.min_amount,
    t.total_amount
FROM orders o
INNER JOIN (SELECT order_id, MIN(amount) AS min_amount, SUM(amount) AS total_amount
    FROM payments
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.total_amount > 4300
