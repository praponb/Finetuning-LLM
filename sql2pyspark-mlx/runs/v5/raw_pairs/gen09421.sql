SELECT id, hire_date, level FROM employees WHERE hire_date = (SELECT MIN(hire_date) FROM employees WHERE level = 'L5')
