SELECT shipped_date AS event_date
FROM shipments
WHERE cost <= 77
INTERSECT
SELECT sale_date AS event_date
FROM store_sales
