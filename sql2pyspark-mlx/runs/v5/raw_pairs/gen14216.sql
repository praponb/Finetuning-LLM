SELECT
    e.id,
    e.order_id,
    e.method,
    e.amount,
    e.status,
    o.channel
FROM payments e
LEFT JOIN orders o
    ON e.order_id = o.id
