WITH remaining_keys AS (
    SELECT id
    FROM employees
    EXCEPT DISTINCT
    SELECT id
    FROM employees
    WHERE level = 'L5'
),
verified AS (
    SELECT s.*
    FROM employees s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    level,
    PERCENTILE_APPROX(salary, 0.95) AS salary_p95,
    COUNT(*) FILTER (WHERE job_title = 'Designer') AS designer_rows,
    COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END) AS engineer_count,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list
FROM verified
GROUP BY DATE_TRUNC('YEAR', hire_date), level;
