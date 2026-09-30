SELECT *, QUARTER(hire_date) AS hire_date_quarter, CAST(salary AS INT) AS salary_int
FROM employees
WHERE manager_id IS NULL OR hire_date >= '2024-06-19'
ORDER BY hire_date, id
LIMIT 10;
