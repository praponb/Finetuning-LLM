SELECT 'shipment' AS kind, delivered_date FROM shipments UNION ALL SELECT 'customer' AS kind, signup_date FROM customers;
