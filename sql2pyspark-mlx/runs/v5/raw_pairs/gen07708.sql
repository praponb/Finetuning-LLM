SELECT 'shipment' AS record_type, delivered_date FROM shipments WHERE status = 'IN_TRANSIT' UNION SELECT 'employee' AS record_type, hire_date FROM employees ORDER BY delivered_date;
