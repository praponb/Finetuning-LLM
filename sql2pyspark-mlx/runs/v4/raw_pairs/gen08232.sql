SELECT email, hire_date, ROUND(salary, 2) AS salary_rounded
FROM employees
WHERE hire_date > '2025-03-01'
