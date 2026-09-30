SELECT s.id, s.shipped_date, o.discount
FROM shipments s
JOIN orders o ON s.order_id = o.id;
