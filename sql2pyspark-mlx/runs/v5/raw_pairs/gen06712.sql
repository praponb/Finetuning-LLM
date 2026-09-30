SELECT 'employee' AS record_type, hire_date FROM employees UNION ALL SELECT 'shipment' AS record_type, shipped_date AS hire_date FROM shipments WHERE YEAR(shipped_date) >= 2025
