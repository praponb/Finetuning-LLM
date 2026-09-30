WITH order_summary AS (
    SELECT order_id, COUNT(DISTINCT amount) unique_orders
    FROM payments
    GROUP BY order_id
)
SELECT
    o.id,
    o.amount,
    o.channel,
    os.unique_orders
FROM orders o
JOIN order_summary os ON o.id = os.order_id
WHERE os.unique_orders > 6;
