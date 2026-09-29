SELECT
    src.id,
    src.order_id,
    src.method,
    src.amount,
    src.status,
    o.shipping_fee
FROM payments src
JOIN orders o
    ON src.order_id = o.id
WHERE o.channel IS NOT NULL
