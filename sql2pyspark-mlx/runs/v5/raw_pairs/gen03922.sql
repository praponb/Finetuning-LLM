SELECT 'customer' AS source, signup_date
FROM customers
UNION ALL
SELECT 'shipment' AS source, shipped_date
FROM shipments
