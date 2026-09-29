SELECT s.id, s.status, s.shipped_date
FROM shipments s
WHERE s.carrier = 'UPS' AND s.order_id NOT IN (SELECT id FROM orders WHERE channel IN ('partner', 'store'))
