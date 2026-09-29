SELECT 'product' kind, launch_date event_date FROM products UNION ALL SELECT 'employee' kind, hire_date event_date FROM employees WHERE NOT is_remote
