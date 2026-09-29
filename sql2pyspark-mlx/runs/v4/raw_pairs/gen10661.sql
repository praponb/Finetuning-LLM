SELECT
    orders.id,
    orders.status,
    orders.amount,
    t.amount_range,
    t.total_count
FROM orders
LEFT OUTER JOIN (SELECT order_id, MAX(amount) - MIN(amount) AS amount_range, COUNT(*) AS total_count
    FROM payments
    GROUP BY order_id) t ON orders.id = t.order_id;
