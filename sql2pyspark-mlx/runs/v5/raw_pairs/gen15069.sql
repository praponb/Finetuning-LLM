SELECT id, hire_date, salary, level FROM employees WHERE hire_date = (SELECT MAX(hire_date) FROM employees) ORDER BY 4, hire_date DESC;
