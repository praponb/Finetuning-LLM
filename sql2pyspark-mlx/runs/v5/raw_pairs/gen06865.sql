SELECT 'shipment' origin, shipped_date FROM shipments WHERE order_id = 9 UNION SELECT 'employee' origin, hire_date FROM employees WHERE job_title IS NOT NULL;
