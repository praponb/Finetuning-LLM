WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM employees
    QUALIFY RANK() OVER (PARTITION BY manager_id ORDER BY salary DESC) = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN salary >= 155000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    SUM(CASE WHEN job_title = 'Engineer' THEN salary ELSE 0 END) AS engineer_salary,
    ROUND(AVG(salary), 2) AS avg_salary,
    PERCENTILE_APPROX(salary, 0.95) AS salary_p95,
    ROUND(AVG(salary), 2) AS avg_salary_2
FROM latest
GROUP BY ROLLUP (CASE WHEN salary >= 155000 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', hire_date))
ORDER BY avg_salary_2 NULLS FIRST;
