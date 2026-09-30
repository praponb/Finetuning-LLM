WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(email)) AS email_title,
        COALESCE(email, 'Unknown') AS email_filled,
        COALESCE(level, 'UNKNOWN') AS resolved_level
    FROM employees
    WHERE manager_id IS NULL
)
-- Grouped metrics
SELECT
    CASE WHEN salary >= 121000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
    level,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    COUNT(*) AS row_count,
    ROUND(AVG(salary), 2) AS avg_salary,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN salary >= 121000 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(*) DESC) AS row_count_dense_rank
FROM valid
GROUP BY CASE WHEN salary >= 121000 THEN 'PREMIUM' ELSE 'STANDARD' END, level
ORDER BY salary_tier DESC;
