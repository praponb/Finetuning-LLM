WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level, manager_id ORDER BY hire_date DESC, id) AS row_num
        FROM employees
    ) t
    WHERE row_num = 1
)
-- Grouped metrics
SELECT
    CASE WHEN salary >= 127000 THEN 'HIGH' ELSE 'LOW' END AS salary_tier,
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    STDDEV_SAMP(salary) AS salary_stddev,
    PERCENTILE_APPROX(salary, 0.5) AS salary_p50,
    COUNT(CASE WHEN job_title = 'Manager' THEN 1 END) AS manager_count,
    GROUPING(CASE WHEN salary >= 127000 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
FROM ranked
GROUP BY CUBE (CASE WHEN salary >= 127000 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', hire_date));
