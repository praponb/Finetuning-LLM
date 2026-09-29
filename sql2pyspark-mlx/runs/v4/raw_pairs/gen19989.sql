SELECT launch_date, 'product' AS source FROM products UNION SELECT hire_date, 'employee' AS source FROM employees ORDER BY launch_date;
