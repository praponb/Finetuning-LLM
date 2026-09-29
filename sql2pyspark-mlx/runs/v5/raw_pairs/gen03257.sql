SELECT
    level,
    is_remote,
    SUM(salary) AS total_salary,
    AVG(salary) AS avg_salary
FROM employees
WHERE LENGTH(full_name) < 12
GROUP BY level, is_remote
ORDER BY avg_salary DESC, level DESC, is_remote
LIMIT 10
