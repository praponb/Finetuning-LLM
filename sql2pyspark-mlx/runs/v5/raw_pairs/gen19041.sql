SELECT hire_date, 'employee' AS kind FROM employees WHERE NOT is_remote UNION SELECT shipped_date, 'shipment' AS kind FROM shipments;
