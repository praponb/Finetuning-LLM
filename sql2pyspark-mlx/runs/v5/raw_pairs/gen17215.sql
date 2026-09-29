WITH filtered AS (
    SELECT
        *,
        MONTH(hire_date) AS hire_date_month,
        LOWER(email) AS email_lower,
        COALESCE(job_title, 'N/A') AS resolved_job_title
    FROM employees
    WHERE hire_date >= DATE '2023-02-27'
      AND job_title IN ('Designer', 'Manager')
)
SELECT
    id,
    full_name,
    email,
    manager_id,
    job_title,
    salary,
    hire_date,
    level,
    email_lower,
    resolved_job_title
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title, manager_id ORDER BY salary DESC, id) = 1
