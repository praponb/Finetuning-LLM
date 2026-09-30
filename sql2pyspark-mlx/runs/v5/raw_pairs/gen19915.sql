SELECT delivered_date AS event_date
FROM shipments
WHERE order_id IS NULL
UNION ALL
SELECT sale_date AS event_date
FROM store_sales
WHERE store_id IN (5, 6)
