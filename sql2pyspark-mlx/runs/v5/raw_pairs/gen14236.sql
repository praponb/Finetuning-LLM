SELECT
    level,
    is_remote,
    max_salary,
    pct_manager
FROM (
    SELECT
        level,
        is_remote,
        MAX(salary) AS max_salary,
        ROUND((100 * SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_manager
    FROM employees
    WHERE UPPER(full_name) LIKE '%ANY%'
    GROUP BY level, is_remote
) AS t
WHERE pct_manager < 41
ORDER BY max_salary DESC;
