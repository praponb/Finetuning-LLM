SELECT
    s.id,
    s.order_id,
    s.shipped_date,
    s.carrier,
    o.store_id,
    o.amount
FROM shipments s
JOIN orders o ON s.order_id = o.id
WHERE o.discount BETWEEN 0.28 AND 0.39
ORDER BY s.shipped_date DESC;
