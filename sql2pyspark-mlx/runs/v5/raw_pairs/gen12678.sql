WITH valid AS (
    SELECT
        *,
        CONCAT_WS('-', job_title, email) AS job_title_email_key,
        SUBSTRING(full_name, 1, 4) AS full_name_prefix,
        COALESCE(job_title, 'NONE') AS job_title_clean
    FROM employees
    WHERE hire_date > DATE '2023-05-21'
      AND level IS NOT NULL
)
SELECT id, dept_id
FROM valid
EXCEPT ALL
SELECT id, dept_id
FROM employees
WHERE is_remote;
