SELECT
    p.id,
    p.paid_at,
    p.method,
    p.order_id,
    o.channel
FROM payments p
LEFT OUTER JOIN orders o ON o.id = p.order_id;
