SELECT hire_date, 'employee' AS source FROM employees UNION SELECT delivered_date, 'shipment' AS source FROM shipments WHERE cost <= 82
