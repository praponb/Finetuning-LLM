SELECT
    s.id,
    s.carrier,
    s.cost,
    s.order_id
FROM shipments s
WHERE s.shipped_date < '2023-05-08' AND s.order_id IN (SELECT id FROM orders WHERE discount <= 0.39)
