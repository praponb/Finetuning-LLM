WITH filtered AS (
    SELECT
        *,
        DATE_FORMAT(hire_date, 'MMM yyyy') AS hire_date_label,
        CONCAT_WS('-', level, email) AS level_email_key,
        COALESCE(job_title, 'UNKNOWN') AS job_title_filled
    FROM employees
    WHERE manager_id IS NOT NULL
)
SELECT
    job_title,
    COUNT(CASE WHEN job_title = 'Director' THEN 1 END) AS director_count,
    SUM(salary) FILTER (WHERE level = 'L5') AS l5_total_salary,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    PERCENTILE_APPROX(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN level = 'L3' THEN 1 END), 0), 2) AS salary_per_l3
FROM filtered
GROUP BY job_title;
