SELECT MAX(salary) - MIN(salary) AS salary_range
FROM employees
WHERE LENGTH(full_name) > 7
ORDER BY salary_range;
