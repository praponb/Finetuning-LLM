SELECT
    o.channel,
    o.order_date,
    s.status,
    s.cost
FROM orders o
INNER JOIN shipments s ON o.id = s.order_id
WHERE MONTH(s.shipped_date) IN (3, 5, 12)
ORDER BY status DESC, o.order_date DESC;
