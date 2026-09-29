WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        UPPER(TRIM(email)) AS email_normalized
    FROM employees
    WHERE hire_date >= DATE '2024-01-22'
      AND job_title IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN salary >= 139000 THEN 'HIGH' ELSE 'LOW' END AS salary_tier,
    SUM(salary) FILTER (WHERE job_title = 'Intern') AS intern_total_salary,
    ROUND(AVG(salary), 2) AS avg_salary,
    SUM(CASE WHEN job_title = 'Director' THEN salary ELSE 0 END) AS director_salary,
    COUNT(*) FILTER (WHERE job_title = 'Director') AS director_rows,
    COUNT_IF(job_title = 'Engineer') AS engineer_events,
    PERCENTILE_APPROX(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles
FROM filtered
GROUP BY ROLLUP (CASE WHEN salary >= 139000 THEN 'HIGH' ELSE 'LOW' END)
HAVING COUNT_IF(job_title = 'Engineer') >= 1;
