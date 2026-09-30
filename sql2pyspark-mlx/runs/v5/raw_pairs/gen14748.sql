SELECT id, order_id
FROM shipments
INTERSECT
SELECT id, order_id
FROM shipments
WHERE YEAR(delivered_date) >= 2025
