SELECT
    level,
    is_remote,
    ROUND(AVG(salary), 2) AS avg_salary,
    MAX(salary) AS max_salary,
    ROUND((100 * SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_designer
FROM employees
GROUP BY level, is_remote
HAVING ROUND((100 * SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END)) / COUNT(*), 1) < 47
