WITH cleaned AS (
    SELECT
        *,
        LENGTH(email) AS email_length,
        INITCAP(TRIM(full_name)) AS full_name_title,
        COALESCE(job_title, 'DIRECT') AS resolved_job_title
    FROM employees
    WHERE job_title IS NOT NULL
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date NULLS LAST, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
enriched AS (
    SELECT
        *,
        LEAD(salary, 1) OVER (PARTITION BY dept_id, level ORDER BY hire_date, id) AS next_salary,
        LAG(email_length, 1, 0) OVER (PARTITION BY job_title ORDER BY salary, id) AS prev_email_length
    FROM deduped
)
SELECT
    job_title,
    APPROX_PERCENTILE(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
    SUM(salary) FILTER (WHERE job_title = 'Director') AS director_total_salary
FROM enriched
GROUP BY job_title
ORDER BY job_title DESC
