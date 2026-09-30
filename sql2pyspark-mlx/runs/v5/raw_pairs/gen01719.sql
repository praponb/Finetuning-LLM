SELECT sale_date AS event_date
FROM store_sales
UNION ALL
SELECT shipped_date AS event_date
FROM shipments
ORDER BY event_date;
