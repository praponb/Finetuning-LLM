SELECT id, hire_date, email
FROM employees
WHERE hire_date = (SELECT MIN(hire_date) FROM employees)
ORDER BY id, email
