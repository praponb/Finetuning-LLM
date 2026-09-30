WITH kept_keys AS (
    -- Set difference on keys
    SELECT manager_id
    FROM employees
    EXCEPT
    SELECT manager_id
    FROM employees
    WHERE manager_id IN (10, 15, 27, 35)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN kept_keys k ON s.manager_id = k.manager_id
)
-- Grouped metrics
SELECT
    CASE WHEN salary >= 126000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
    SUM(salary) AS total_salary,
    SIZE(COLLECT_SET(level)) AS num_levels,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    COUNT(CASE WHEN level = 'L5' THEN 1 END) AS l5_count,
    COUNT(*) AS cnt,
    ROUND(AVG(salary), 2) AS avg_salary
FROM reconciled
GROUP BY 1
ORDER BY total_salary
