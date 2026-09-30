SELECT hire_date AS event_date, 'employee' AS source FROM employees UNION SELECT signup_date AS event_date, 'customer' AS source FROM customers WHERE LOWER(name) LIKE '%rac%';
