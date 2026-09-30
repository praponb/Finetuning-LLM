SELECT id, hire_date, level, email FROM employees WHERE hire_date = (SELECT MIN(hire_date) FROM employees) ORDER BY 3
