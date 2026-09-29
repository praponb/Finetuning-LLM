SELECT email, 'customer' AS source FROM customers UNION ALL SELECT email, 'employee' AS source FROM employees WHERE YEAR(hire_date) = 2024 ORDER BY source;
