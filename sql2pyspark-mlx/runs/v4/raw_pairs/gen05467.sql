SELECT
    s.id,
    s.order_id,
    s.cost,
    o.channel
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
ORDER BY s.cost, o.channel DESC, id
LIMIT 50
