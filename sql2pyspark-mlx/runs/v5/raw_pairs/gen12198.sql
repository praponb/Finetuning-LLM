SELECT
    p.method,
    p.order_id,
    o.discount,
    o.channel,
    o.amount,
    CONCAT(o.channel, ', ', o.status) AS channel_status
FROM payments p
JOIN orders o ON p.order_id = o.id
WHERE o.customer_id = 35 AND o.store_id IS NOT NULL
