SELECT shipped_date, 'shipment' AS source FROM shipments UNION ALL SELECT hire_date, 'employee' AS source FROM employees;
