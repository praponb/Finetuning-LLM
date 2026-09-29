SELECT *, CEIL(salary) AS salary_whole, LENGTH(level) AS level_length
FROM employees
WHERE salary > 109000
ORDER BY id DESC, 2
