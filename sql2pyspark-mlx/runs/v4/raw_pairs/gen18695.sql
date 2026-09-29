SELECT
    is_remote,
    level,
    COUNT(*) AS row_count,
    AVG(salary) AS avg_salary
FROM employees
WHERE full_name IS NULL AND email LIKE '%@startup.io'
GROUP BY is_remote, level;
