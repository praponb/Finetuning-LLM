SELECT s.id, s.carrier, s.shipped_date, s.status, o.discount FROM shipments s INNER JOIN orders o ON o.id = s.order_id ORDER BY discount DESC;
