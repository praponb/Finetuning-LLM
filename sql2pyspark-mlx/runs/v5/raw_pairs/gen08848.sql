SELECT id, delivered_date, order_id
FROM shipments
WHERE carrier IN ('DHL', 'UPS', 'Flash') OR delivered_date BETWEEN '2024-10-28' AND '2024-11-29'
ORDER BY order_id
