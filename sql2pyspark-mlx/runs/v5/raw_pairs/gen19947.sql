SELECT shipped_date AS event_date
FROM shipments
WHERE order_id IN (21, 36)
INTERSECT
SELECT sale_date AS event_date
FROM store_sales
ORDER BY event_date;
