SELECT id, delivered_date
FROM shipments
WHERE delivered_date <= (SELECT MIN(delivered_date) FROM shipments WHERE order_id IS NOT NULL)
ORDER BY delivered_date ASC;
