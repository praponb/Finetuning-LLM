SELECT id, hire_date, salary FROM employees WHERE hire_date = (SELECT MIN(hire_date) FROM employees) ORDER BY hire_date DESC, id ASC LIMIT 5;
