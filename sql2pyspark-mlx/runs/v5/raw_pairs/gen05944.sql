SELECT signup_date, 'customer' AS record_type
FROM customers
UNION ALL
SELECT shipped_date, 'shipment' AS record_type
FROM shipments
ORDER BY record_type DESC
