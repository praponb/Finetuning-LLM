SELECT status
FROM orders
INTERSECT
SELECT status
FROM shipments
WHERE delivered_date > '2023-06-01'
