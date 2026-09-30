WITH scored AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY job_title, dept_id ORDER BY salary, id) salary_bucket,
        LAST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_job_title,
        ROW_NUMBER() OVER w_ordered event_seq
    FROM employees
    WINDOW w_ordered AS (ORDER BY salary, id)
)
SELECT
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    COUNT(*) AS event_count,
    SUM(CASE WHEN level = 'L5' THEN salary_bucket ELSE 0 END) AS l5_salary_bucket
FROM scored
GROUP BY DATE_TRUNC('MONTH', hire_date);
