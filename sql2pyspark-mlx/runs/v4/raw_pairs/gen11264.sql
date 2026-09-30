SELECT id, hire_date, salary, is_remote FROM employees WHERE hire_date < (SELECT MIN(hire_date) FROM employees WHERE is_remote) ORDER BY 1 DESC LIMIT 5
