SELECT launch_date, 'product' AS source FROM products UNION SELECT hire_date, 'employee' AS source FROM employees WHERE dept_id = 10 ORDER BY source;
