SELECT
    id,
    level,
    dept_id,
    FLOOR(salary) AS salary_whole
FROM employees
WHERE dept_id = 5
ORDER BY dept_id ASC, id ASC
LIMIT 10;
