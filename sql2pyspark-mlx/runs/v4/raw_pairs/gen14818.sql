WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(job_title, ' ', email) AS job_title_email,
        CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label,
        COALESCE(level, 'NONE') AS level_filled
    FROM employees
    WHERE hire_date > TO_DATE('2023-05-16')
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN salary >= 123000 THEN 'HIGH' ELSE 'LOW' END AS salary_tier,
    PERCENTILE_APPROX(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN level = 'L5' THEN 1 END), 0), 2) AS salary_per_l5
FROM cleaned
GROUP BY CUBE (CASE WHEN salary >= 123000 THEN 'HIGH' ELSE 'LOW' END)
HAVING salary_per_l5 > 0
ORDER BY salary_per_l5 DESC NULLS FIRST
