SELECT id, hire_date, RANK() OVER (ORDER BY hire_date) rnk
FROM employees;
