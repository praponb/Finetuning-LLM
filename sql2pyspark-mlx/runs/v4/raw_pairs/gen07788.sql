WITH matched_keys AS (
    -- Set difference on keys
    SELECT job_title, level
    FROM employees
    EXCEPT DISTINCT
    SELECT job_title, level
    FROM employees
    WHERE is_remote
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    JOIN matched_keys k ON s.job_title = k.job_title AND s.level = k.level
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', hire_date) AS week_start,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN level = 'L1' THEN 1 END), 0), 2) AS salary_per_l1
FROM reconciled
GROUP BY DATE_TRUNC('WEEK', hire_date)
ORDER BY distinct_ids DESC;
