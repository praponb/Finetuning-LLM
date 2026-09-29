SELECT
    s.id,
    s.status,
    o.amount,
    o.channel,
    o.discount
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
WHERE o.channel IN ('web', 'mobile', 'partner')
