WITH payments_by_order AS (
    SELECT order_id, MAX(amount) - MIN(amount) amount_range, AVG(amount) avg_amount
    FROM payments
    GROUP BY order_id
)
SELECT
    orders.status,
    orders.customer_id,
    payments_by_order.amount_range,
    payments_by_order.avg_amount
FROM orders
JOIN payments_by_order ON orders.id = payments_by_order.order_id
ORDER BY orders.status DESC, payments_by_order.amount_range
