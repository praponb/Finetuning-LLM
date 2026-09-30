WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC NULLS LAST, id) AS latest_rank
        FROM employees
    ) x
    WHERE latest_rank = 1
)
-- Grouped metrics
SELECT
    CASE WHEN salary >= 150000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    ROUND(AVG(salary), 2) AS avg_salary,
    ROUND(AVG(salary), 2) AS avg_salary_2,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list
FROM deduped
GROUP BY CASE WHEN salary >= 150000 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', hire_date)
ORDER BY salary_tier DESC NULLS LAST;
