SELECT is_remote, avg_salary
FROM (
    SELECT is_remote, ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    GROUP BY is_remote
) AS agg
WHERE avg_salary > 40
