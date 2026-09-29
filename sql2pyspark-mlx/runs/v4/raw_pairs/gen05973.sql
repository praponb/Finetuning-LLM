WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level, manager_id ORDER BY salary DESC, id) = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN salary >= 149000 THEN 'HIGH' ELSE 'LOW' END AS salary_tier,
    APPROX_PERCENTILE(salary, 0.5) AS salary_p50,
    ROUND(AVG(salary), 2) AS avg_salary,
    APPROX_PERCENTILE(salary, 0.5) AS salary_p50_2,
    SUM(CASE WHEN job_title = 'Designer' THEN salary ELSE 0 END) AS designer_salary
FROM latest
GROUP BY 1
HAVING salary_p50_2 > 0;
