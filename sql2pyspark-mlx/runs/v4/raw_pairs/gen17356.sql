WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        dept_id,
        salary,
        hire_date,
        is_remote,
        level,
        CAST(is_remote AS INT) AS is_remote_flag,
        COALESCE(job_title, 'DIRECT') AS job_title_filled
    FROM employees
    WHERE hire_date > DATE '2023-04-19'
      AND job_title = 'Analyst'
)
-- Grouped metrics
SELECT
    level,
    SUM(CASE WHEN level = 'L5' THEN is_remote_flag ELSE 0 END) AS l5_is_remote_flag,
    ROUND(AVG(is_remote_flag), 2) AS avg_is_remote_flag,
    PERCENTILE_APPROX(salary, 0.9) AS salary_p90,
    COUNT(DISTINCT level) AS distinct_levels,
    COUNT(*) AS cnt
FROM cleaned
GROUP BY level
HAVING SUM(CASE WHEN level = 'L5' THEN is_remote_flag ELSE 0 END) < 0
ORDER BY level NULLS FIRST;
