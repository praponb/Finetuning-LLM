SELECT order_id
FROM shipments
WHERE delivered_date <= '2024-12-01'
INTERSECT
SELECT order_id
FROM shipments
WHERE order_id = 21;
