WITH with_dims AS (
    SELECT
        src.*,
        e.job_title AS employee_job_title,
        e.level AS employee_level,
        e.full_name AS employee_full_name
    FROM employees src
    LEFT JOIN employees e
        ON src.manager_id = e.id
    WHERE e.hire_date BETWEEN '2023-08-01' AND '2023-12-01'
)
SELECT
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    AVG(salary) AS avg_salary,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN employee_job_title = 'Engineer' THEN 1 END), 0), 2) AS salary_per_engineer
FROM with_dims
GROUP BY 1
HAVING AVG(salary) > 0
ORDER BY avg_salary DESC
