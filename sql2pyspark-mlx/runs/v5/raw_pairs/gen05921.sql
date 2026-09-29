SELECT id, manager_id, ROUND(salary * 1.15, 2) AS salary_adjusted
FROM employees
WHERE full_name LIKE 'bo%';
