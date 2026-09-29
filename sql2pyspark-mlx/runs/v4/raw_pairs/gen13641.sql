SELECT delivered_date AS event_date
FROM shipments
WHERE status IN ('LOST', 'RETURNED')
UNION
SELECT launch_date AS event_date
FROM products;
