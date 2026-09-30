WITH kept_keys AS (
    -- Set intersection on keys
    SELECT job_title, dept_id
    FROM employees
    INTERSECT
    SELECT job_title, dept_id
    FROM employees
    WHERE LENGTH(full_name) < 5
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN kept_keys k ON s.job_title = k.job_title AND s.dept_id = k.dept_id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    SUM(CASE WHEN level = 'L5' THEN salary ELSE 0 END) AS l5_salary,
    SIZE(COLLECT_SET(level)) AS num_levels,
    COUNT(CASE WHEN level = 'L5' THEN 1 END) AS l5_count
FROM kept_rows
GROUP BY DATE_TRUNC('MONTH', hire_date);
